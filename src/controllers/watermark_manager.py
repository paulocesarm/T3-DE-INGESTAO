"""WatermarkManager - Gerencia watermarks para carga incremental

Responsabilidades:
- Ler última watermark da tabela Delta control_watermarks
- Criar filtro MongoDB baseado na watermark
- Atualizar watermark após ingestão bem-sucedida
- Inicializar tabela de watermarks se não existir
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any


class WatermarkManager:
    """Gerenciador de watermarks para carga incremental"""
    
    def __init__(self, config: Dict, spark: SparkSession):
        """
        Args:
            config: Configurações da pipeline
            spark: SparkSession ativa
        """
        self.config = config
        self.spark = spark
        self.watermark_table = config['control']['watermark_table']
        
        self._ensure_watermark_table_exists()
    
    def _ensure_watermark_table_exists(self) -> None:
        """Cria tabela de watermarks se não existir"""
        schema = StructType([
            StructField("collection", StringType(), False),
            StructField("watermark_field", StringType(), False),
            StructField("last_value", StringType(), True),
            StructField("updated_at", TimestampType(), False)
        ])
        
        # Verifica se tabela existe
        try:
            self.spark.sql(f"SELECT 1 FROM {self.watermark_table} LIMIT 1")
            print(f"[OK] Tabela {self.watermark_table} já existe")
        except:
            # Cria tabela vazia
            df = self.spark.createDataFrame([], schema)
            df.write.format("delta").mode("overwrite").saveAsTable(self.watermark_table)
            print(f"[OK] Tabela {self.watermark_table} criada")
    
    def get_last_watermark(
        self,
        collection_name: str,
        watermark_field: str
    ) -> Optional[Any]:
        """Recupera última watermark persistida
        
        Args:
            collection_name: Nome da coleção
            watermark_field: Campo usado como watermark (ex: 'date')
            
        Returns:
            Valor da última watermark ou None se primeira carga
        """
        query = f"""
            SELECT last_value
            FROM {self.watermark_table}
            WHERE collection = '{collection_name}'
              AND watermark_field = '{watermark_field}'
            ORDER BY updated_at DESC
            LIMIT 1
        """
        
        result = self.spark.sql(query).collect()
        
        if result and result[0]['last_value']:
            watermark_value = result[0]['last_value']
            return watermark_value
        else:
            return None
    
    def build_incremental_filter(
        self,
        watermark_field: str,
        watermark_value: Optional[Any]
    ) -> Dict:
        """Constrói filtro MongoDB para carga incremental
        
        Args:
            watermark_field: Campo de watermark
            watermark_value: Valor da última watermark
            
        Returns:
            Filtro MongoDB (ex: {"date": {"$gt": "2024-01-01"}})
        """
        if watermark_value is None:
            # Primeira carga - sem filtro
            return {}
        
        # Converter watermark STRING → datetime para comparação correta com ISODate do MongoDB
        from dateutil import parser
        if isinstance(watermark_value, str):
            try:
                watermark_value = parser.isoparse(watermark_value)
            except:
                pass  # Se falhar conversão, usa string mesmo
        
        # Carga incremental - apenas registros NOVOS
        # Usa $gt (greater than) para não duplicar o último registro
        mongo_filter = {
            watermark_field: {"$gt": watermark_value}
        }
        
        return mongo_filter
    
    def update_watermark(
        self,
        collection_name: str,
        watermark_field: str,
        new_value: Any
    ) -> None:
        """Atualiza watermark após ingestão bem-sucedida
        
        Args:
            collection_name: Nome da coleção
            watermark_field: Campo de watermark
            new_value: Novo valor máximo da watermark
        """
        from pyspark.sql.functions import current_timestamp, lit
        
        # Cria registro de atualização
        update_data = [(
            collection_name,
            watermark_field,
            str(new_value),
            datetime.now()
        )]
        
        df = self.spark.createDataFrame(
            update_data,
            ["collection", "watermark_field", "last_value", "updated_at"]
        )
        
        # Append na tabela de watermarks
        df.write.format("delta").mode("append").saveAsTable(self.watermark_table)
    
    def get_max_value_from_batch(
        self,
        batch_data: list,
        watermark_field: str
    ) -> Optional[Any]:
        """Extrai valor máximo do campo watermark do lote extraído
        
        Args:
            batch_data: Lista de documentos MongoDB
            watermark_field: Campo de watermark
            
        Returns:
            Valor máximo encontrado ou None
        """
        if not batch_data:
            return None
        
        # Extrai todos os valores do campo watermark
        values = [
            doc.get(watermark_field)
            for doc in batch_data
            if watermark_field in doc and doc[watermark_field] is not None
        ]
        
        if not values:
            return None
        
        # Retorna o maior valor (assume que campo é ordenável)
        return max(values)