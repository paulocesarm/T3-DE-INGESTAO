# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,MongoDB Connection Setup
# MAGIC %md
# MAGIC # MongoDB Connection Setup
# MAGIC
# MAGIC Configura secret scope e valida conectividade com MongoDB Atlas.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - MongoDB Atlas com database `sample_mflix` carregado
# MAGIC - Network Access liberado (0.0.0.0/0)
# MAGIC - Databricks CLI instalado localmente
# MAGIC
# MAGIC **Execução:** Uma vez antes da primeira carga.

# COMMAND ----------

# DBTITLE 1,Step 1: Validar se Scope Existe
# Valida se o scope 'mongodb_scope' existe
# (o scope deve ser criado via CLI ou REST API)

try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    if 'mongodb_scope' in scopes:
        print("[OK] Secret scope 'mongodb_scope' encontrado")
    else:
        print("[ERRO] Secret scope 'mongodb_scope' NÃO encontrado")
        print("")
        print("Crie o scope via Databricks CLI:")
        print("  databricks secrets create-scope mongodb_scope")
        print("")
        print("Ou via REST API (veja célula markdown abaixo)")
except Exception as e:
    print(f"[ERRO] Não foi possível listar scopes: {e}")

# COMMAND ----------

# DBTITLE 1,Step 2: Criar Scope e Adicionar Secret
# MAGIC %md
# MAGIC ## Step 2: Criar Scope e Adicionar Secret
# MAGIC
# MAGIC **Escolha 1 das 2 opções abaixo:**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 OPÇÃO A: Via Databricks CLI (Recomendado)
# MAGIC
# MAGIC **No seu terminal local:**
# MAGIC
# MAGIC **1. Instalar CLI** (se não tiver):
# MAGIC ```bash
# MAGIC pip install databricks-cli
# MAGIC ```
# MAGIC
# MAGIC **2. Configurar autenticação** (primeira vez):
# MAGIC ```bash
# MAGIC databricks configure --token
# MAGIC ```
# MAGIC - Host: `https://dbc-8aab7158-ae06.cloud.databricks.com`
# MAGIC - Token: Gere em "User Settings → Developer → Access Tokens"
# MAGIC
# MAGIC **3. Criar o scope:**
# MAGIC ```bash
# MAGIC databricks secrets create-scope mongodb_scope
# MAGIC ```
# MAGIC
# MAGIC **4. Adicionar connection string:**
# MAGIC ```bash
# MAGIC databricks secrets put-secret --scope mongodb_scope --key mongodb_connection_string
# MAGIC ```
# MAGIC → Abrirá um editor. Cole sua connection string e salve (Ctrl+X no nano, :wq no vim)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ OPÇÃO B: Via Código Temporário (Rápido)
# MAGIC
# MAGIC **Execute a célula Python abaixo** (depois DELETE ela para não deixar senha visível!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Após criar o secret, execute a **Célula 1** para validar.

# COMMAND ----------

# DBTITLE 1,Step 3: Validar Conectividade
# MAGIC %pip install pymongo --quiet
# MAGIC
# MAGIC import pymongo
# MAGIC import ssl
# MAGIC
# MAGIC # Buscar connection string do secret
# MAGIC conn_str = dbutils.secrets.get(scope="mongodb_scope", key="mongodb_connection_string")
# MAGIC
# MAGIC print("Validando conexão com MongoDB...")
# MAGIC print("")
# MAGIC
# MAGIC try:
# MAGIC     # Conectar com parâmetros SSL apropriados para Databricks
# MAGIC     client = pymongo.MongoClient(
# MAGIC         conn_str,
# MAGIC         serverSelectionTimeoutMS=30000,
# MAGIC         connectTimeoutMS=30000,
# MAGIC         socketTimeoutMS=30000,
# MAGIC         tls=True,
# MAGIC         tlsAllowInvalidCertificates=True  # Necessário para Databricks Serverless
# MAGIC     )
# MAGIC     
# MAGIC     # Testar conexão
# MAGIC     client.admin.command('ping')
# MAGIC     print("[OK] Conexão estabelecida com MongoDB")
# MAGIC     print("")
# MAGIC     
# MAGIC     # Validar database
# MAGIC     dbs = client.list_database_names()
# MAGIC     if 'sample_mflix' not in dbs:
# MAGIC         print("[ERRO] Database 'sample_mflix' não encontrado")
# MAGIC         print(f"       Disponíveis: {dbs}")
# MAGIC     else:
# MAGIC         print("[OK] Database 'sample_mflix' encontrado")
# MAGIC         print("")
# MAGIC         
# MAGIC         # Validar coleções
# MAGIC         db = client['sample_mflix']
# MAGIC         existing = set(db.list_collection_names())
# MAGIC         expected = {'comments', 'movies', 'users', 'theaters', 'sessions', 'embedded_movies'}
# MAGIC         
# MAGIC         missing = expected - existing
# MAGIC         if missing:
# MAGIC             print(f"[AVISO] Coleções faltando: {sorted(missing)}")
# MAGIC             print(f"        Disponíveis: {sorted(existing)}")
# MAGIC         else:
# MAGIC             print(f"[OK] Todas as {len(expected)} coleções esperadas presentes:")
# MAGIC             total_docs = 0
# MAGIC             for col in sorted(expected):
# MAGIC                 count = db[col].estimated_document_count()
# MAGIC                 total_docs += count
# MAGIC                 print(f"     - {col}: {count:,} docs")
# MAGIC             
# MAGIC             print(f"[OK] Total: ~{total_docs:,} documentos")
# MAGIC             print("")
# MAGIC             print("=" * 60)
# MAGIC             print("✅ SETUP COMPLETO! Todas as validações passaram.")
# MAGIC             print("=" * 60)
# MAGIC     
# MAGIC     client.close()
# MAGIC     
# MAGIC except pymongo.errors.ServerSelectionTimeoutError as e:
# MAGIC     print("[ERRO] Timeout ao conectar no MongoDB")
# MAGIC     print("")
# MAGIC     print("Causas possíveis:")
# MAGIC     print("  1. Network Access não liberado no Atlas (adicione 0.0.0.0/0)")
# MAGIC     print("  2. Cluster pausado")
# MAGIC     print("  3. Firewall bloqueando conexões SSL na porta 27017")
# MAGIC     print("")
# MAGIC     print(f"Detalhe técnico: {e}")
# MAGIC     
# MAGIC except pymongo.errors.OperationFailure as e:
# MAGIC     print(f"[ERRO] Falha de autenticação: {e}")
# MAGIC     print("       Verifique usuário/senha na connection string")
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     print(f"[ERRO] Inesperado: {e}")
# MAGIC     raise e
# MAGIC
# MAGIC print("")
# MAGIC print("📌 Próximo passo: Execute a CÉLULA 6 para criar o schema Bronze")

# COMMAND ----------

# DBTITLE 1,Step 4: Criar Schema Bronze
# Cria schema bronze se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

print("[OK] Schema bronze criado")
print("Setup completo. Próximo: notebooks/02_run_ingestion_pipeline.py")