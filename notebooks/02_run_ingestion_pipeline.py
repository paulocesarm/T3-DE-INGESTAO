# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Pipeline de Ingestão
# MAGIC %md
# MAGIC # Pipeline de Ingestão MongoDB → Bronze
# MAGIC
# MAGIC **Coleções:**
# MAGIC - comments (incremental)
# MAGIC - movies, users, theaters, sessions, embedded_movies (full)

# COMMAND ----------

# DBTITLE 1,Instalar dependências
# Instalar bibliotecas necessárias
%pip install pymongo tenacity pyyaml python-dateutil
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Carregar configurações
import yaml
import json
import sys

# Adiciona src ao path
sys.path.append("/Workspace/Users/paulocesarmlf@gmail.com/T3-DE-INGESTAO/src")

# Carrega pipeline_config.yaml
with open("/Workspace/Users/paulocesarmlf@gmail.com/T3-DE-INGESTAO/config/pipeline_config.yaml", "r") as f:
    pipeline_config = yaml.safe_load(f)

# Carrega collections.json
with open("/Workspace/Users/paulocesarmlf@gmail.com/T3-DE-INGESTAO/config/collections.json", "r") as f:
    collections_config = json.load(f)

print("[OK] Configurações carregadas")
print(f"[INFO] Database: {pipeline_config['mongodb']['database']}")
print(f"[INFO] Coleções: {len(collections_config['collections'])}")

# COMMAND ----------

# DBTITLE 1,Importar IngestionController
# Reimporta módulos (caso tenham sido atualizados)
import importlib
import sys

# Remove módulos do cache para forçar reimportação
for module in list(sys.modules.keys()):
    if module.startswith('controllers') or module.startswith('extractors'):
        del sys.modules[module]

from controllers.ingestion_controller import IngestionController

# Inicializa controller
controller = IngestionController(config=pipeline_config, spark=spark)

print("[OK] IngestionController inicializado")

# COMMAND ----------

# DBTITLE 1,Executar ingestão de TODAS as coleções
# Executa pipeline para todas as coleções
for collection in collections_config['collections']:
    try:
        controller.run_ingestion(collection)
    except Exception as e:
        print(f"ERRO: {collection['name']} | {e}")
