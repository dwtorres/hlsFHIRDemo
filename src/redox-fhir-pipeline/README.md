# Redox FHIR Ingestion Pipeline

A production-ready pipeline for ingesting FHIR bundles from Redox into Databricks, making them available as realtime analytics tables.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOURCE: Mount (/mnt/redox/fhir/)                                           │
│  Redox FHIR JSON Bundles                                                    │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ File Arrival Trigger
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER                                                               │
│  ┌─────────────────┐    ┌──────────────────────┐                           │
│  │ fhir_bronze     │───▶│ fhir_bronze_variant  │                           │
│  │ (Raw STRING)    │    │ (Parsed VARIANT)     │                           │
│  └─────────────────┘    └──────────┬───────────┘                           │
│                                    │                                        │
│  ┌─────────────────┐    ┌──────────┴───────────┐    ┌──────────────────┐   │
│  │ bundle_meta     │◀───│ resources_exploded   │───▶│ resource_schemas │   │
│  │ (Bundle PK)     │    │ (Key-Value Pairs)    │    │ (Inferred Types) │   │
│  └─────────────────┘    └──────────────────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼ Dynamic for-each
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Per Resource Type)                                           │
│  ┌──────────┐ ┌───────────┐ ┌──────────────┐ ┌──────────┐ ┌─────────────┐  │
│  │ Patient  │ │ Claim     │ │ Organization │ │ Coverage │ │ Practitioner│  │
│  │ (FK→BM)  │ │ (FK→BM)   │ │ (FK→BM)      │ │ (FK→BM)  │ │ (FK→BM)     │  │
│  └──────────┘ └───────────┘ └──────────────┘ └──────────┘ └─────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `00 - Configuration.ipynb` | Set catalog, schema, and mount path |
| `01 - Bronze Ingestion.ipynb` | Ingest raw FHIR bundles as STRING and VARIANT |
| `02 - Bundle Meta.ipynb` | Extract bundle-level metadata with primary key |
| `03 - Resources Exploded.ipynb` | Flatten entries, infer schemas |
| `04 - Silver Tables.ipynb` | Create per-resource streaming tables |
| `05 - Discover Resource Types.ipynb` | Set task values for workflow |
| `06 - Drop Tables.ipynb` | Utility for full refresh |

## Quick Start

### 1. Configure Your Environment

Edit the defaults in `00 - Configuration.ipynb`:

```sql
DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT 'your_catalog';
DECLARE OR REPLACE VARIABLE schema_use STRING DEFAULT 'your_schema';
DECLARE OR REPLACE VARIABLE mount_path STRING DEFAULT '/mnt/your/redox/path/';
```

### 2. Create the Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS your_catalog;
CREATE SCHEMA IF NOT EXISTS your_catalog.your_schema;
```

### 3. Run Notebooks in Order

1. `01 - Bronze Ingestion.ipynb` - Creates `fhir_bronze` and `fhir_bronze_variant`
2. `02 - Bundle Meta.ipynb` - Creates `bundle_meta`
3. `03 - Resources Exploded.ipynb` - Creates `resources_exploded` and `resource_schemas`
4. `05 - Discover Resource Types.ipynb` - Lists available resource types
5. `04 - Silver Tables.ipynb` - Run once per resource type (e.g., Patient, Claim)

### 4. Deploy as Workflow

Use the job configuration in `resources/redox_fhir_pipeline.job.yml`:

```bash
databricks bundle deploy -t your_target
```

## Key Features

### Streaming Tables
All tables use `CREATE OR REFRESH STREAMING TABLE` for continuous incremental processing.

### VARIANT Data Type
Uses Databricks VARIANT for schema-agnostic JSON handling - no predefined schemas required.

### Automatic Schema Discovery
The `resource_schemas` table automatically discovers fields for each FHIR resource type.

### Change Data Feed
All tables have CDF enabled for downstream CDC consumption.

### Foreign Key Constraints
Silver tables reference `bundle_meta` via foreign key for data integrity and lineage.

## Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `catalog_use` | Unity Catalog name | `redox_fhir` |
| `schema_use` | Schema name | `bronze` |
| `mount_path` | Mount path for FHIR files | `/mnt/redox/fhir/` |
| `full_refresh` | Drop and recreate tables | `false` |
| `resource_type` | Resource type for Silver table | `Patient` |

## Querying Data

### List All Bundles
```sql
SELECT bundle_uuid, bundle_id, entry_count, ingest_time
FROM bundle_meta
ORDER BY ingest_time DESC;
```

### Query Patient Resources
```sql
SELECT
  patient_uuid,
  name,
  birthDate,
  gender
FROM Patient
LIMIT 10;
```

### Join Resources Across Bundle
```sql
SELECT
  p.name AS patient_name,
  c.status AS claim_status,
  c.created AS claim_date
FROM Patient p
JOIN Claim c ON p.bundle_uuid = c.bundle_uuid
WHERE c.use = 'preauthorization';
```

### Track New Records via CDF
```sql
SELECT *
FROM table_changes('Patient', 1)
WHERE _change_type = 'insert';
```

## Troubleshooting

### Schema Evolution
If new fields appear in FHIR data, the pipeline will detect them and prompt for a full refresh:

```sql
-- Check for new columns
SELECT column_name FROM resource_schemas
WHERE resource_type = 'Patient'
EXCEPT
SELECT column_name FROM information_schema.columns
WHERE table_name = 'patient';
```

### Full Refresh
To completely reset the pipeline:

1. Run `06 - Drop Tables.ipynb`, or
2. Set `full_refresh=true` in job parameters

### Malformed JSON
The pipeline uses `try_parse_json()` which returns NULL for malformed JSON instead of failing:

```sql
-- Find bundles with parse errors
SELECT bundle_uuid, file_metadata.file_name
FROM fhir_bronze b
LEFT JOIN fhir_bronze_variant v USING (bundle_uuid)
WHERE v.fhir IS NULL;
```

## Performance Tuning

- **Concurrency**: The workflow runs up to 20 Silver table creations in parallel
- **Trigger Interval**: Default 600s between triggers (adjust in job config)
- **Serverless**: Uses Serverless SQL Warehouse for managed compute
