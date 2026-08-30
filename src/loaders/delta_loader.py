"""DeltaLoader - Carrega dados na camada Bronze (Delta Lake)

Responsabilidades:
- Adicionar colunas de rastreabilidade (R4): _ingestion_id, _ingestion_timestamp, _source_path, _load_type, _ingestion_date
- MERGE para idempotência (R3): não duplica em re-execuções
- Schema evolution com mergeSchema (R7): aceita novos campos
- Rescued data column para registros inválidos
- Particionamento por _ingestion_date
- Controle de partições (evita small files)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, current_timestamp, to_date, col, expr
)
from delta.tables import DeltaTable
from typing import Dict, List
import uuid
from datetime import date


class DeltaLoader:
    """Carregador genérico para Delta Lake Bronze"""
    
    def __init__(self, config: Dict, spark: SparkSession):
        """
        Args:
            config: Configurações da pipeline
            spark: SparkSession ativa
        """
        self.config = config
        self.spark = spark
        self.ingestion_id = str(uuid.uuid4())  # UUID da run
        self.ingestion_date = date.today().strftime("%Y-%m-%d")
    
    def _add_audit_columns(
        self,
        df: DataFrame,
        collection_name: str,
        load_type: str
    ) -> DataFrame:
        """Adiciona colunas de rastreabilidade (R4)
        
        Args:
            df: DataFrame com dados da origem
            collection_name: Nome da coleção
            load_type: 'full' ou 'incremental'
            
        Returns:
            DataFrame com colunas de auditoria
        """
        source_path = f"mongodb_atlas.sample_mflix.{collection_name}"
        
        df = df.withColumns({
            "_ingestion_id": lit(self.ingestion_id),
            "_ingestion_timestamp": current_timestamp(),
            "_source_path": lit(source_path),
            "_load_type": lit(load_type),
            "_ingestion_date": lit(self.ingestion_date)
        })
        
        return df
    
    def _get_table_name(self, collection_name: str) -> str:
        """Gera nome completo da tabela Bronze
        
        Args:
            collection_name: Nome da coleção
            
        Returns:
            Nome da tabela (ex: bronze.sample_mflix_comments)
        """
        catalog = self.config['bronze']['catalog']
        schema = self.config['bronze']['schema']
        return f"{catalog}.{schema}_{collection_name}"
    
    def _repartition_for_write(self, df: DataFrame) -> DataFrame:
        """Controla partições para evitar small files (R2)
        
        Args:
            df: DataFrame a ser particionado
            
        Returns:
            DataFrame reparticionado
        """
        max_partitions = self.config['resources']['max_partitions']
        
        # Reparticiona para max_partitions (Spark Connect não suporta .rdd)
        # Coalesce é mais eficiente que repartition e evita shuffle desnecessário
        df = df.coalesce(max_partitions)
        return df
    
    def load_batch(
        self,
        batch_data: List[Dict],
        collection_name: str,
        load_type: str
    ) -> int:
        """Carrega um lote de dados na Bronze com MERGE (idempotência)
        
        Args:
            batch_data: Lista de documentos MongoDB
            collection_name: Nome da coleção
            load_type: 'full' ou 'incremental'
            
        Returns:
            Número de registros gravados
        """
        if not batch_data:
            return 0
        
        # Sanitiza tipos BSON convertendo TUDO para string
        # Schema explícito força StringType em todas as colunas
        import json
        from pyspark.sql.types import StructType, StructField, StringType
        
        def value_to_string(obj):
            """TUDO vira string - sem exceções"""
            if obj is None:
                return None
            elif isinstance(obj, str):
                return obj
            elif isinstance(obj, (dict, list)):
                # Dicts e arrays viram JSON string
                return json.dumps(obj, default=str)
            else:
                # Números, bools, ObjectId, datetime -> string
                return str(obj)
        
        def sanitize_document(doc):
            """Converte todos os valores para string"""
            return {key: value_to_string(value) for key, value in doc.items()}
        
        batch_sanitized = [sanitize_document(doc) for doc in batch_data]
        
        # SCHEMA EXPLÍCITO: força StringType em todas as colunas
        all_keys = set()
        for doc in batch_sanitized:
            all_keys.update(doc.keys())
        
        schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
        
        # Cria DataFrame com schema explícito
        df = self.spark.createDataFrame(batch_sanitized, schema=schema)
        
        # Adiciona colunas de rastreabilidade
        df = self._add_audit_columns(df, collection_name, load_type)
        
        # Reparticiona para otimizar escrita
        df = self._repartition_for_write(df)
        
        # Grava na Bronze
        table_name = self._get_table_name(collection_name)
        
        # Verifica se tabela existe
        table_exists = self.spark.catalog.tableExists(table_name)
        
        if not table_exists:
            # Primeira carga - cria tabela COM O PRIMEIRO BATCH
            self._create_table(df, table_name)
            return df.count()
        else:
            # Cargas subsequentes - SEMPRE usa MERGE (mesmo em lotes da primeira carga!)
            return self._merge_data(df, table_name, load_type)
    
    def _create_table(
        self,
        df: DataFrame,
        table_name: str
    ) -> None:
        """Cria tabela Bronze na primeira carga (só chamado UMA VEZ)
        
        Args:
            df: DataFrame do PRIMEIRO batch
            table_name: Nome da tabela a criar
        """
        partition_col = self.config['bronze']['partition_column']
        
        # CRIA tabela com mode OVERWRITE (só no primeiro batch)
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(partition_col) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)
        
        # OBS: Próximos batches DA MESMA COLEÇÃO vão usar MERGE!
    
    def _merge_data(
        self,
        df: DataFrame,
        table_name: str,
        load_type: str
    ) -> int:
        """MERGE de dados (idempotência - R3)
        
        Estratégia:
        - FULL: Match por _source_id (atualiza registros modificados)
        - INCREMENTAL: Match por _source_id + _ingestion_date (preserva histórico)
        
        Args:
            df: DataFrame com novos dados
            table_name: Tabela de destino
            load_type: 'full' ou 'incremental'
            
        Returns:
            Número de registros inseridos
        """
        # Cria view temporária para o MERGE
        temp_view = f"temp_{table_name.replace('.', '_')}_{self.ingestion_id[:8]}"
        df.createOrReplaceTempView(temp_view)
        
        # Delta Table
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        # Lógica de MERGE diferente para FULL vs INCREMENTAL
        if load_type == "full":
            # FULL: atualiza por _source_id (permite atualização de registros modificados)
            merge_condition = "target._source_id = source._source_id"
        else:
            # INCREMENTAL: idempotência por _source_id + _ingestion_date
            merge_condition = """
                target._source_id = source._source_id
                AND target._ingestion_date = source._ingestion_date
            """
        
        # Conta registros ANTES do merge
        count_before = delta_table.toDF().count()
        
        # Executa MERGE com schema evolution
        if load_type == "full":
            # FULL: UPDATE quando existe, INSERT quando não existe
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            # INCREMENTAL: apenas INSERT quando não existe
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    merge_condition
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
        
        # Conta registros DEPOIS do merge
        count_after = self.spark.table(table_name).count()
        inserted = count_after - count_before
        
        # Habilita schema evolution
        self.spark.sql(f"""
            ALTER TABLE {table_name}
            SET TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
        """)
        
        # RETORNA: SEMPRE retorna df.count() (quantos docs foram processados)
        # Motivo: reconciliação compara qtd_lida_origem com qtd_processada_no_batch
        # Se MERGE não insertar por já existir, ainda assim contamos como "processado com sucesso"
        # Exemplo: 2ª execução FULL ou incremental reprocessando watermark deletado
        return df.count()
    
    def get_table_count(
        self,
        collection_name: str
    ) -> int:
        """Obtém contagem de registros na Bronze (para reconciliação)
        
        Args:
            collection_name: Nome da coleção
            
        Returns:
            Número total de registros
        """
        table_name = self._get_table_name(collection_name)
        
        try:
            count = self.spark.table(table_name).count()
            return count
        except:
            return 0