# Pipeline de Ingestão MongoDB → Bronze

**Objetivo**: Extrair 6 coleções do MongoDB Atlas (`sample_mflix`) para Delta Lake (camada Bronze).

## Estrutura

```
config/
  ├── pipeline_config.yaml    # Configurações
  └── collections.json        # Parâmetros das 6 coleções

src/
  ├── extractors/mongodb_extractor.py
  ├── loaders/delta_loader.py
  ├── controllers/ingestion_controller.py
  ├── controllers/watermark_manager.py
  └── validators/quality_checker.py

notebooks/
  ├── 01_setup_secrets.py           # Setup inicial (1x)
  └── 02_run_ingestion_pipeline.py  # Execução
```

## Como Executar

### 1. Setup (1x apenas)

Execute notebook `01_setup_secrets` para salvar connection string do MongoDB.

### 2. Dropar tudo (se quiser recomeçar)

No notebook `02_run_ingestion_pipeline`, execute **célula 1** (DROP).

### 3. Executar pipeline

No mesmo notebook:
- Célula 3: carregar configs
- Célula 4: criar controller
- Célula 7: **EXECUTAR INGESTÃO**

A mesma célula 7 roda sempre (1ª, 2ª, 3ª... execução).

## Coleções

- **comments** (50k docs) - INCREMENTAL por campo `date`
- **movies** (21k docs) - FULL
- **users** (185 docs) - FULL
- **theaters** (1.5k docs) - FULL
- **sessions** (poucos docs) - FULL
- **embedded_movies** (3.5k docs) - FULL

## Tabelas Criadas

- `bronze.sample_mflix_*` (6 tabelas)
- `bronze.control_ingestion_log` (log de execuções)
- `bronze.control_watermarks` (para incremental)

## Consultas

```sql
-- Ver execuções
SELECT collection, load_type, qtd_lida_origem, qtd_gravada_destino, status
FROM bronze.control_ingestion_log
ORDER BY start_time DESC;

-- Ver watermark (incremental)
SELECT * FROM bronze.control_watermarks;
```