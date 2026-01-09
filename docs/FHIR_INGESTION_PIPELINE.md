# FHIR Ingestion Pipeline - Detailed Documentation

## Overview

The **Synthea FHIR Ingestion** job (`workshop_synthea_fhir_ingestion`) is a Databricks workflow that implements a **medallion architecture** (Bronze → Silver) for ingesting and processing HL7 FHIR (Fast Healthcare Interoperability Resources) JSON bundles into queryable Delta tables.

### Key Characteristics

| Property | Value |
|----------|-------|
| **Job Name** | `Synthea FHIR Ingestion` |
| **Architecture** | Medallion (Bronze → Silver) |
| **Processing Model** | Streaming Tables with incremental refresh |
| **Compute** | Serverless SQL Warehouse |
| **Trigger** | File arrival in landing zone |
| **Concurrency** | Up to 42 parallel Silver tasks |

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         FHIR Ingestion Pipeline                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────────┐                                                           │
│  │  Landing Zone    │  /Volumes/{catalog}/{schema}/synthetic_files_raw/         │
│  │  (FHIR Bundles)  │  output/fhir/*.json                                       │
│  └────────┬─────────┘                                                           │
│           │                                                                      │
│           ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐       │
│  │                         BRONZE LAYER                                  │       │
│  │  ┌─────────────────┐    ┌─────────────────────┐    ┌──────────────┐  │       │
│  │  │  fhir_bronze    │───▶│  fhir_bronze_variant │───▶│  bundle_meta │  │       │
│  │  │  (raw text)     │    │  (parsed VARIANT)    │    │  (metadata)  │  │       │
│  │  └─────────────────┘    └──────────┬──────────┘    └──────────────┘  │       │
│  └───────────────────────────────────┬──────────────────────────────────┘       │
│                                      │                                           │
│                                      ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐       │
│  │                      RESOURCE EXPLOSION                               │       │
│  │  ┌─────────────────────┐    ┌─────────────────────────┐              │       │
│  │  │   fhir_resources    │───▶│   fhir_resource_schemas │              │       │
│  │  │   (exploded rows)   │    │   (inferred schemas)    │              │       │
│  │  └──────────┬──────────┘    └─────────────────────────┘              │       │
│  └─────────────┬────────────────────────────────────────────────────────┘       │
│                │                                                                 │
│                ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐       │
│  │                         SILVER LAYER                                  │       │
│  │  ┌───────────┐ ┌───────────┐ ┌─────────────┐ ┌────────────┐         │       │
│  │  │  Patient  │ │ Encounter │ │ Observation │ │ Condition  │  ...    │       │
│  │  └───────────┘ └───────────┘ └─────────────┘ └────────────┘         │       │
│  │                                                                       │       │
│  │  (Up to 42 resource types processed in parallel)                     │       │
│  └──────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Job Task Breakdown

### Task 1: `full_refresh_conditional`
**Type**: Condition Task
**Purpose**: Determines if a full refresh is requested

```yaml
condition_task:
  op: EQUAL_TO
  left: "{{job.parameters.full_refresh}}"
  right: "true"
```

**Behavior**:
- If `full_refresh=true` → Proceed to `drop_streaming_tables`
- If `full_refresh=false` → Skip to `bronze` task

---

### Task 2: `drop_streaming_tables`
**Type**: SQL Notebook
**Notebook**: `w00 - Drop Streaming Tables.ipynb`
**Executes Only If**: `full_refresh_conditional` outcome is `true`

**Purpose**: Drops all streaming tables to enable a clean rebuild

**Tables Dropped**:
```sql
DROP TABLE IF EXISTS fhir_bronze;
DROP TABLE IF EXISTS fhir_bronze_variant;
DROP TABLE IF EXISTS fhir_resources;
DROP TABLE IF EXISTS fhir_resource_schemas;
DROP TABLE IF EXISTS bundle_meta;
```

**When to Use**:
- Schema changes in FHIR bundles
- Data corruption recovery
- Complete re-processing of historical data

---

### Task 3: `bronze`
**Type**: SQL Notebook
**Notebook**: `w01 - Bronze.ipynb`
**Dependencies**: `drop_streaming_tables` OR `full_refresh_conditional=false`

**Purpose**: Ingests raw FHIR JSON bundles from the landing zone

#### Table: `fhir_bronze`
**Description**: Raw FHIR bundles stored as full text strings

| Column | Type | Description |
|--------|------|-------------|
| `file_metadata` | STRUCT | Original file metadata (path, name, size, timestamps) |
| `ingest_time` | TIMESTAMP | When the file was ingested |
| `bundle_uuid` | STRING | Unique identifier generated for each bundle |
| `value` | STRING | Complete JSON bundle as raw text |

**Key Features**:
- Uses `read_files()` with `wholeText => true` to preserve complete JSON
- Streaming table with Change Data Feed enabled
- Deletion vectors and row tracking for efficient updates

#### Table: `fhir_bronze_variant`
**Description**: Parsed FHIR bundles as VARIANT data type

| Column | Type | Description |
|--------|------|-------------|
| `bundle_uuid` | STRING | Unique identifier for the FHIR bundle |
| `ingest_time` | TIMESTAMP | Ingestion timestamp |
| `file_metadata` | STRUCT | Original file metadata |
| `fhir` | VARIANT | Fully parsed JSON as queryable VARIANT |

**Key Features**:
- Uses `try_parse_json()` for safe JSON parsing
- Enables SQL path navigation (e.g., `fhir:entry`, `fhir:resourceType`)
- Streams from `fhir_bronze` for incremental processing

---

### Task 4: `meta`
**Type**: SQL Notebook
**Notebook**: `w02a - Meta.ipynb`
**Dependencies**: `bronze`

**Purpose**: Extracts bundle-level metadata

#### Table: `bundle_meta`
**Description**: Metadata about each FHIR bundle

| Column | Type | Description |
|--------|------|-------------|
| `bundle_uuid` | STRING (PK) | Unique identifier, primary key for all joins |
| `file_metadata` | STRUCT | Original file metadata |
| `ingest_time` | TIMESTAMP | Ingestion timestamp |
| `bundle_resourceType` | STRING | Bundle's resource type (usually "Bundle") |
| `bundle_type` | STRING | Bundle type (e.g., "transaction", "collection") |
| `meta` | VARIANT | Optional metadata about the bundle source |

**Key Features**:
- Primary key constraint on `bundle_uuid`
- All resource tables reference this table via foreign key
- Critical for joining resources across the same bundle

---

### Task 5: `resources_exploded`
**Type**: SQL Notebook
**Notebook**: `w02b - Resources.ipynb`
**Dependencies**: `bronze`

**Purpose**: Explodes FHIR bundle entries into individual resource rows

#### Table: `fhir_resources`
**Description**: Exploded key-value pairs from all FHIR resources

| Column | Type | Description |
|--------|------|-------------|
| `resource_uuid` | STRING (PK) | SHA-256 hash of bundle_uuid + fullUrl |
| `bundle_uuid` | STRING (FK) | Reference to parent bundle |
| `fullUrl` | STRING | Full URL identifier for the resource |
| `resourceType` | STRING | Type of FHIR resource (Patient, Encounter, etc.) |
| `pos` | INT | Position in the resource element array |
| `key` | STRING | Name of the resource element (becomes column name) |
| `value` | VARIANT | Value of the resource element |

**SQL Logic**:
```sql
SELECT
  sha2(concat(bundle_uuid, entry.value:fullUrl::string), 256) as resource_uuid,
  bundle_uuid,
  CAST(entry.value:fullUrl AS STRING) as fullUrl,
  CAST(entry.value:resource.resourceType AS STRING) as resourceType,
  resource.*
FROM
  STREAM(fhir_bronze_variant),
  LATERAL variant_explode(fhir:entry) as entry,
  LATERAL variant_explode(entry.value:resource) as resource
```

**Key Features**:
- Uses `LATERAL variant_explode()` to flatten nested FHIR structures
- Creates one row per resource element (key-value pair)
- Enables dynamic schema discovery

#### Table: `fhir_resource_schemas`
**Description**: Aggregated schemas for each resource type

| Column | Type | Description |
|--------|------|-------------|
| `resourceType` | STRING | FHIR resource type |
| `column_name` | STRING | Element name (future column name) |
| `schema_of_variant` | STRING | Inferred schema from VARIANT |
| `schema_as_struct` | STRING | Schema with STRUCT instead of OBJECT |

**Purpose**: Enables dynamic DDL generation for Silver tables

---

### Task 6: `available_resources`
**Type**: Python Notebook
**Notebook**: `w03 - Task Values.ipynb`
**Dependencies**: `resources_exploded`
**Compute**: Serverless Job Compute (not SQL Warehouse)

**Purpose**: Discovers which FHIR resource types exist in the data

**Logic**:
```python
resource_types = spark.sql(
    "SELECT DISTINCT resourceType FROM fhir_resource_schemas"
).collect()
resource_types = [row.resourceType for row in resource_types]
dbutils.jobs.taskValues.set("resource_types", resource_types)
```

**Output**: Task value `resource_types` containing list like:
```python
['Patient', 'Encounter', 'Observation', 'Condition', 'Procedure',
 'MedicationRequest', 'DiagnosticReport', 'Immunization', ...]
```

**Why This Matters**: Enables dynamic parallel processing of only the resource types that actually exist in the data.

---

### Task 7: `silver`
**Type**: For-Each Task (Parallel)
**Notebook**: `w04 - Silver.ipynb`
**Dependencies**: `available_resources`
**Concurrency**: 42 parallel iterations

**Purpose**: Creates dedicated streaming tables for each FHIR resource type

**Configuration**:
```yaml
for_each_task:
  inputs: "{{tasks.available_resources.values.resource_types}}"
  concurrency: 42
  task:
    task_key: silver_iteration
    notebook_task:
      notebook_path: w04 - Silver.ipynb
      base_parameters:
        resource_type: "{{input}}"
```

**For Each Resource Type**, the notebook:

1. **Checks for Schema Evolution**:
   ```sql
   -- Detects new columns not yet in the table
   SELECT column_name FROM fhir_resource_schemas
   WHERE resourceType = :resource_type
   EXCEPT
   SELECT column_name FROM information_schema.columns
   WHERE table_name = lower(:resource_type)
   ```

2. **Generates Dynamic DDL**:
   - Creates column definitions from `fhir_resource_schemas`
   - Adds foreign key constraints to `bundle_meta` and `fhir_resources`
   - All data columns stored as VARIANT for flexibility

3. **Creates/Refreshes Streaming Table**:
   ```sql
   CREATE OR REFRESH STREAMING TABLE {resource_type} (
     {resource_type}_uuid STRING PRIMARY KEY,
     bundle_uuid STRING,
     {resource_type}_url STRING,
     {element1} VARIANT,
     {element2} VARIANT,
     ...
   )
   AS SELECT * FROM (
     SELECT resource_uuid, bundle_uuid, fullUrl, key, value
     FROM STREAM(fhir_resources)
     WHERE resourceType = '{resource_type}'
   )
   PIVOT (first(value) FOR key IN ({columns}))
   ```

**Example Output Tables**:

| Table | Description |
|-------|-------------|
| `Patient` | Demographics, identifiers, addresses, contacts |
| `Encounter` | Visit/admission records, status, participants |
| `Observation` | Lab results, vital signs, clinical measurements |
| `Condition` | Diagnoses, problems, health concerns |
| `Procedure` | Clinical procedures performed |
| `MedicationRequest` | Medication orders and prescriptions |
| `DiagnosticReport` | Lab reports, imaging reports |
| `Immunization` | Vaccination records |
| `Claim` | Insurance claims |
| `ExplanationOfBenefit` | Insurance payment details |
| ... | (and more based on data) |

---

## Data Flow Summary

```
1. FHIR JSON Bundle arrives in landing zone
   │
   ▼
2. fhir_bronze: Store raw JSON as text
   │
   ▼
3. fhir_bronze_variant: Parse JSON to VARIANT
   │
   ├──▶ 4a. bundle_meta: Extract bundle metadata
   │
   └──▶ 4b. fhir_resources: Explode entries to key-value rows
              │
              └──▶ fhir_resource_schemas: Infer schemas per resource type
                     │
                     ▼
5. available_resources: Get list of resource types
   │
   ▼
6. Silver tables (parallel): One table per resource type
   Patient, Encounter, Observation, Condition, ...
```

---

## Job Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog_use` | `${var.catalog_use}` | Unity Catalog catalog name |
| `schema_use` | `${var.schema_use}` | Schema name for tables |
| `full_refresh` | `${var.full_refresh}` | If "true", drops and rebuilds all tables |

---

## Trigger Configuration

```yaml
trigger:
  pause_status: PAUSED  # or UNPAUSED for auto-trigger
  file_arrival:
    url: /Volumes/{catalog}/{schema}/synthetic_files_raw/output/fhir/
    min_time_between_triggers_seconds: 600   # 10 minutes
    wait_after_last_change_seconds: 60       # Wait for batch completion
```

**Behavior**:
- Monitors the landing zone for new FHIR bundle files
- Waits 60 seconds after last file change before triggering
- Minimum 10 minutes between job runs

---

## Key Technologies Used

| Technology | Purpose |
|------------|---------|
| **Streaming Tables** | Incremental processing with exactly-once semantics |
| **VARIANT Data Type** | Schema-flexible storage for complex nested JSON |
| **Change Data Feed** | Track changes for downstream consumers |
| **Deletion Vectors** | Efficient deletes without full rewrites |
| **Row Tracking** | Audit trail for data lineage |
| **LATERAL variant_explode()** | Flatten nested arrays in SQL |
| **PIVOT** | Transform rows to columns dynamically |
| **For-Each Task** | Parallel processing with dynamic inputs |

---

## Typical Runtime

| Phase | Duration | Notes |
|-------|----------|-------|
| Bronze ingestion | 2-5 min | Depends on file count/size |
| Meta extraction | 1-2 min | Single pass over bronze |
| Resource explosion | 3-7 min | Complex nested processing |
| Available resources | < 1 min | Simple distinct query |
| Silver (parallel) | 5-15 min | 42 concurrent tasks |
| **Total** | **10-25 min** | For typical Synthea dataset |

---

## Validation Queries

After pipeline completion, verify data with:

```sql
-- Check bronze ingestion
SELECT COUNT(*) as bundle_count FROM fhir_bronze;

-- List all Silver tables created
SHOW TABLES LIKE '*';

-- Sample patient data
SELECT * FROM Patient LIMIT 10;

-- Count resources by type
SELECT resourceType, COUNT(*) as count
FROM fhir_resources
GROUP BY resourceType
ORDER BY count DESC;

-- Verify foreign key relationships
SELECT
  p.patient_uuid,
  e.encounter_uuid,
  bm.bundle_type
FROM Patient p
JOIN Encounter e ON p.bundle_uuid = e.bundle_uuid
JOIN bundle_meta bm ON p.bundle_uuid = bm.bundle_uuid
LIMIT 10;
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| No data in Silver tables | Resource type not in data | Check `fhir_resource_schemas` |
| Schema evolution errors | New elements in FHIR | Set `full_refresh=true` |
| JSON parse failures | Malformed FHIR bundles | Check `fhir_bronze` for raw data |
| Slow performance | Large file backlog | Increase warehouse size |
| Missing foreign keys | Out-of-order processing | Ensure `bundle_meta` completes first |

---

## Related Files

| File | Purpose |
|------|---------|
| `resources/workshop_synthea_fhir_ingestion.job.yml` | Job definition |
| `src/FHIR_Workshop/w00 - Drop Streaming Tables.ipynb` | Full refresh cleanup |
| `src/FHIR_Workshop/w01 - Bronze.ipynb` | Raw ingestion |
| `src/FHIR_Workshop/w02a - Meta.ipynb` | Bundle metadata |
| `src/FHIR_Workshop/w02b - Resources.ipynb` | Resource explosion |
| `src/FHIR_Workshop/w03 - Task Values.ipynb` | Resource discovery |
| `src/FHIR_Workshop/w04 - Silver.ipynb` | Dynamic Silver tables |

---

*Document generated: 2026-01-08*
*Pipeline version: hlsFHIRDemo/dwtorres target*
