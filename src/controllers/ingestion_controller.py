"""IngestionController - Orquestrador principal da pipeline

Responsabilidades:
- Carregar configurações de collections.json e pipeline_config.yaml
- Instanciar MongoDBExtractor, DeltaLoader, WatermarkManager, QualityChecker
- Orquestrar fluxo completo: connect -> extract -> load -> validate -> log
- Gravar control_ingestion_log com 12+ campos (R5)
- Tratamento de exceções e determinação de status
- Atualizar watermark apenas em SUCCESS
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from datetime import datetime
from typing import Dict, Optional
import traceback
import sys
import os

# Imports das classes criadas
sys.path.append("/Workspace/Users/paulocesarmlf@gmail.com/T3-DE-INGESTAO/src")

from extractors.mongodb_extractor import MongoDBExtractor
from loaders.delta_loader import DeltaLoader
from controllers.watermark_manager import WatermarkManager
from validators.quality_checker import QualityChecker


class IngestionController:
    """Controlador principal da pipeline de ingestão"""
    
    def __init__(self, config: Dict, spark: SparkSession):
        """
        Args:
            config: Configurações completas da pipeline
            spark: SparkSession ativa
        """
        self.config = config
        self.spark = spark
        
        # Inicializa componentes
        self.extractor = MongoDBExtractor(config, spark)
        self.loader = DeltaLoader(config, spark)
        self.watermark_manager = WatermarkManager(config, spark)
        self.quality_checker = QualityChecker(config)
        
        self.control_log_table = config['control']['log_table']
        self._ensure_control_log_exists()
    
    def _ensure_control_log_exists(self) -> None:
        """Cria tabela de controle de execuções se não existir (R5)"""
        schema = StructType([
            StructField("_ingestion_id", StringType(), False),
            StructField("collection", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("watermark_inicial", StringType(), True),
            StructField("watermark_final", StringType(), True),
            StructField("qtd_lida_origem", IntegerType(), False),
            StructField("qtd_gravada_destino", IntegerType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duracao_seg", FloatType(), True),
            StructField("status", StringType(), False),
            StructField("mensagem_erro", StringType(), True),
            StructField("divergencia_percentual", FloatType(), True)
        ])
        
        try:
            self.spark.sql(f"SELECT 1 FROM {self.control_log_table} LIMIT 1")
            print(f"[OK] Tabela {self.control_log_table} já existe")
        except:
            df = self.spark.createDataFrame([], schema)
            df.write.format("delta").mode("overwrite").saveAsTable(self.control_log_table)
            print(f"[OK] Tabela {self.control_log_table} criada")
    
    def run_ingestion(
        self,
        collection_config: Dict
    ) -> None:
        """Executa ingestão de uma coleção
        
        Args:
            collection_config: Configuração da coleção (do collections.json)
        """
        collection_name = collection_config['name']
        load_type = collection_config['load_type']
        watermark_field = collection_config.get('watermark_field')
        projection = collection_config.get('projection', {})
        
        # Log simplificado no início (removido)
        
        start_time = datetime.now()
        ingestion_id = self.loader.ingestion_id
        
        watermark_inicial = None
        watermark_final = None
        qtd_lida_origem = 0
        qtd_gravada_destino = 0
        status = "SUCCESS"
        mensagem_erro = None
        divergencia_pct = 0.0
        
        try:
            # 1. Conecta ao MongoDB
            self.extractor.connect()
            
            # 2. Gerenciar watermark (se incremental)
            filter_query = {}
            if load_type == "incremental" and watermark_field:
                watermark_inicial = self.watermark_manager.get_last_watermark(
                    collection_name, watermark_field
                )
                filter_query = self.watermark_manager.build_incremental_filter(
                    watermark_field, watermark_inicial
                )
            
            # 3. Obter contagem da origem (para reconciliação)
            qtd_lida_origem = self.extractor.get_collection_count(
                collection_name, filter_query if filter_query else None
            )
            
            if qtd_lida_origem == 0:
                status = "SUCCESS"
            else:
                # 4. Extrair e carregar em lotes
                max_watermark_value = None
                total_loaded = 0
                
                for batch in self.extractor.extract_batch(
                    collection_name=collection_name,
                    projection=projection,
                    filter_query=filter_query if filter_query else None,
                    sort_field=watermark_field
                ):
                    # Carregar lote na Bronze
                    loaded_count = self.loader.load_batch(
                        batch_data=batch,
                        collection_name=collection_name,
                        load_type=load_type
                    )
                    
                    total_loaded += loaded_count
                    
                    # Atualizar watermark máximo do lote
                    if watermark_field:
                        batch_max = self.watermark_manager.get_max_value_from_batch(
                            batch, watermark_field
                        )
                        if batch_max:
                            if max_watermark_value is None or batch_max > max_watermark_value:
                                max_watermark_value = batch_max
                
                qtd_gravada_destino = total_loaded
                watermark_final = max_watermark_value
                
                # 5. Validação final (reconciliação)
                is_valid, divergencia_pct, reconcile_msg = self.quality_checker.reconcile_final(
                    total_read_from_source=qtd_lida_origem,
                    total_written_to_bronze=qtd_gravada_destino,
                    collection_name=collection_name
                )
                
                if not is_valid:
                    status = "FAILED"
                    mensagem_erro = reconcile_msg
                else:
                    # 6. Atualizar watermark (apenas em SUCCESS)
                    if load_type == "incremental" and watermark_field and watermark_final:
                        self.watermark_manager.update_watermark(
                            collection_name, watermark_field, watermark_final
                        )
            
        except Exception as e:
            status = "FAILED"
            mensagem_erro = f"{type(e).__name__}: {str(e)}"
            print(f"\n[ERRO FATAL] {mensagem_erro}")
            traceback.print_exc()
        
        finally:
            # Fecha conexão MongoDB
            self.extractor.close()
            
            # 7. Registrar execução no log de controle (R5)
            end_time = datetime.now()
            duracao_seg = (end_time - start_time).total_seconds()
            
            self._log_execution(
                ingestion_id=ingestion_id,
                collection=collection_name,
                load_type=load_type,
                watermark_inicial=str(watermark_inicial) if watermark_inicial else None,
                watermark_final=str(watermark_final) if watermark_final else None,
                qtd_lida_origem=qtd_lida_origem,
                qtd_gravada_destino=qtd_gravada_destino,
                start_time=start_time,
                end_time=end_time,
                duracao_seg=duracao_seg,
                status=status,
                mensagem_erro=mensagem_erro,
                divergencia_percentual=divergencia_pct
            )
            
            # Log final simplificado
            print(f"{collection_name}, {load_type}, {qtd_lida_origem} linhas")
    
    def _log_execution(
        self,
        ingestion_id: str,
        collection: str,
        load_type: str,
        watermark_inicial: Optional[str],
        watermark_final: Optional[str],
        qtd_lida_origem: int,
        qtd_gravada_destino: int,
        start_time: datetime,
        end_time: datetime,
        duracao_seg: float,
        status: str,
        mensagem_erro: Optional[str],
        divergencia_percentual: float
    ) -> None:
        """Registra execução na tabela de controle (R5)
        
        Grava um registro com todos os metadados da execução
        """
        # Schema explícito para evitar CANNOT_DETERMINE_TYPE com valores None
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, 
            DoubleType, TimestampType
        )
        
        schema = StructType([
            StructField("_ingestion_id", StringType(), False),
            StructField("collection", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("watermark_inicial", StringType(), True),
            StructField("watermark_final", StringType(), True),
            StructField("qtd_lida_origem", IntegerType(), False),
            StructField("qtd_gravada_destino", IntegerType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("duracao_seg", DoubleType(), False),
            StructField("status", StringType(), False),
            StructField("mensagem_erro", StringType(), True),
            StructField("divergencia_percentual", DoubleType(), False)
        ])
        
        log_data = [(
            ingestion_id,
            collection,
            load_type,
            watermark_inicial,
            watermark_final,
            qtd_lida_origem,
            qtd_gravada_destino,
            start_time,
            end_time,
            duracao_seg,
            status,
            mensagem_erro,
            divergencia_percentual
        )]
        
        df = self.spark.createDataFrame(log_data, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(self.control_log_table)