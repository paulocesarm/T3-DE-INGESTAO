"""MongoDBExtractor - Extrai dados do MongoDB com boas práticas de recursos

Responsabilidades:
- Conexão via Databricks Secrets (sem credenciais hardcoded)
- Leitura paginada com batchSize (evita OOM)
- Projection pushdown (traz apenas campos necessários)
- Retry com backoff exponencial
- Iterator de cursor (sem collect())
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Dict, Optional, List, Iterator, Any
from bson import ObjectId
from datetime import datetime
import time


class MongoDBExtractor:
    """Extrator genérico de dados do MongoDB"""
    
    def __init__(self, config: Dict, spark_session):
        """
        Args:
            config: Dicionário com configurações (pipeline_config.yaml)
            spark_session: SparkSession ativa
        """
        self.config = config
        self.spark = spark_session
        self.client = None
        self.db = None
    
    @staticmethod
    def _sanitize_document(doc: Dict) -> Dict:
        """Converte tipos BSON complexos para tipos Python simples compatÃ­veis com Spark
        
        Args:
            doc: Documento MongoDB com tipos BSON
            
        Returns:
            Documento com tipos Python simples (str, int, float, dict, list)
        """
        if not isinstance(doc, dict):
            if isinstance(doc, ObjectId):
                return str(doc)
            elif isinstance(doc, datetime):
                return doc.isoformat()
            elif isinstance(doc, list):
                return [MongoDBExtractor._sanitize_document(item) for item in doc]
            else:
                return doc
        
        sanitized = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                sanitized[key] = str(value)
            elif isinstance(value, datetime):
                sanitized[key] = value.isoformat()
            elif isinstance(value, dict):
                sanitized[key] = MongoDBExtractor._sanitize_document(value)
            elif isinstance(value, list):
                sanitized[key] = [MongoDBExtractor._sanitize_document(item) for item in value]
            else:
                # int, float, str, bool, None mantÃ©m como estÃ¡
                sanitized[key] = value
        
        return sanitized
        
    def _get_connection_string(self) -> str:
        """Recupera connection string do Databricks Secrets
        
        Returns:
            Connection string do MongoDB Atlas
        """
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(self.spark)
        
        scope = self.config['mongodb']['secret_scope']
        key = self.config['mongodb']['secret_key']
        
        connection_string = dbutils.secrets.get(scope=scope, key=key)
        return connection_string
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True
    )
    def connect(self) -> None:
        """Estabelece conexão com MongoDB com retry automático
        
        Raises:
            ConnectionFailure: Se não conseguir conectar após 3 tentativas
        """
        connection_string = self._get_connection_string()
        
        self.client = MongoClient(
            connection_string,
            maxPoolSize=self.config['resources']['connection_pool_size'],
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        # Testa conexão
        self.client.admin.command('ping')
        
        # Seleciona database
        database_name = self.config['mongodb']['database']
        self.db = self.client[database_name]
        self.database_name = database_name
    
    def extract_batch(
        self,
        collection_name: str,
        projection: Dict = None,
        filter_query: Dict = None,
        sort_field: str = None
    ) -> Iterator[List[Dict]]:
        """Extrai dados em lotes usando cursor (evita collect)
        
        Args:
            collection_name: Nome da coleção
            projection: Campos a serem extraídos (pushdown)
            filter_query: Filtro MongoDB (ex: {"date": {"$gt": watermark}})
            sort_field: Campo para ordenação (usado em incremental)
            
        Yields:
            Lotes de documentos (listas)
        """
        if self.db is None:
            raise RuntimeError("Conexão não estabelecida. Execute connect() primeiro.")
        
        collection = self.db[collection_name]
        batch_size = self.config['resources']['batch_size']
        
        # Pipeline de agregação (mais eficiente que find)
        pipeline = []
        
        if filter_query:
            pipeline.append({"$match": filter_query})
        
        if sort_field:
            pipeline.append({"$sort": {sort_field: 1}})
        
        if projection:
            pipeline.append({"$project": projection})
        
        # Cursor com batchSize para leitura paginada
        cursor = collection.aggregate(pipeline, batchSize=batch_size)
        
        batch = []
        total_docs = 0
        
        try:
            for doc in cursor:
                # Converte ObjectId para string (Spark não suporta BSON)
                if '_id' in doc:
                    doc['_source_id'] = str(doc['_id'])
                    del doc['_id']
                
                # Sanitiza tipos BSON ANTES de adicionar ao batch
                doc = self._sanitize_document(doc)
                batch.append(doc)
                
                # Yield quando batch está cheio
                if len(batch) >= batch_size:
                    total_docs += len(batch)
                    yield batch
                    batch = []
            
            # Yield do último batch (pode ter menos que batch_size)
            if batch:
                total_docs += len(batch)
                yield batch
            
        except PyMongoError as e:
            print(f"[ERRO] Falha ao extrair de {collection_name}: {e}")
            raise
    
    def get_collection_count(
        self,
        collection_name: str,
        filter_query: Dict = None
    ) -> int:
        """Obtém contagem de documentos (para reconciliação)
        
        Args:
            collection_name: Nome da coleção
            filter_query: Filtro MongoDB
            
        Returns:
            Número de documentos
        """
        if self.db is None:
            raise RuntimeError("Conexão não estabelecida.")
        
        collection = self.db[collection_name]
        
        if filter_query:
            count = collection.count_documents(filter_query)
        else:
            count = collection.estimated_document_count()
        
        return count
    
    def close(self) -> None:
        """Fecha conexão com MongoDB"""
        if self.client:
            self.client.close()