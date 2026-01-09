# Quick Implementation Guide: Optimized inject_bad_data

## Overview

This guide provides step-by-step instructions to replace the slow inject_bad_data task with the optimized Spark-native version.

---

## Prerequisites

- Access to Databricks workspace
- Permissions to update job configurations
- Access to Synthea pipeline job

---

## Step 1: Deploy Optimized Notebook

### Option A: Upload via Workspace UI

1. Navigate to Databricks workspace
2. Go to `Workspace` → `Users` → `<your-user>`
3. Create folder: `synthea_optimization`
4. Click **Upload**
5. Select file: `/Users/dwtorres/src/work/hlsFHIRDemo/notebooks/synthea_optimization/inject_bad_data_optimized.py`

### Option B: Upload via Databricks CLI

```bash
cd /Users/dwtorres/src/work/hlsFHIRDemo

# Upload to workspace
databricks workspace import \
  notebooks/synthea_optimization/inject_bad_data_optimized.py \
  /Users/<your-user>/synthea_optimization/inject_bad_data_optimized \
  --language PYTHON \
  --format SOURCE
```

---

## Step 2: Update Job Configuration

### Find Current Job Configuration

1. Go to **Workflows** in Databricks UI
2. Search for Synthea pipeline job
3. Click **Edit** or view job YAML
4. Locate `inject_bad_data` task

### Current Configuration (External GitHub)

```yaml
- task_key: inject_bad_data
  depends_on:
    - task_key: generate_synthea_data
  notebook_task:
    source: GIT
    notebook_path: notebooks/01-data-generation/2.0-inject-bad-data
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_name: synthea
  timeout_seconds: 14400  # 4 hours
  max_retries: 1
```

### Updated Configuration (Local Optimized)

```yaml
- task_key: inject_bad_data
  depends_on:
    - task_key: generate_synthea_data
  notebook_task:
    notebook_path: /Users/<your-user>/synthea_optimization/inject_bad_data_optimized
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_name: synthea
  timeout_seconds: 600  # 10 minutes (50x faster)
  max_retries: 2
  new_cluster:  # Or use existing cluster
    spark_version: "14.3.x-scala2.12"
    node_type_id: "Standard_DS4_v2"
    num_workers: 4
    spark_conf:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
```

### Apply Changes

**Via UI**:
1. Click **Edit** on the job
2. Navigate to `inject_bad_data` task
3. Update **Notebook path** to local optimized version
4. Update **Timeout** to 600 seconds
5. Click **Save**

**Via Bundle (if using Databricks Asset Bundle)**:
1. Edit `bundles/data_engineering/resources/jobs/synthea_pipeline.yml`
2. Update task configuration as shown above
3. Deploy:
   ```bash
   cd bundles/data_engineering
   databricks bundle deploy --target devtest
   ```

---

## Step 3: Test with Small Dataset

### Create Test Dataset

```python
# In a Databricks notebook
from pyspark.sql import functions as F

# Create small test directory with subset of data
source_path = "/Volumes/{catalog}/{schema}/synthetic_files_raw/output/csv/"
test_path = "/Volumes/{catalog}/{schema}/synthetic_files_raw/output/csv_test/"

# Copy first directory only for testing
directories = spark.sql(f"LIST '{source_path}' ").orderBy("name").limit(1).collect()
for directory in directories:
    src_dir = directory[0]
    dest_dir = test_path + directory[1]
    dbutils.fs.cp(src_dir, dest_dir, recurse=True)
```

### Run Test

1. Update job parameters to use test path:
   ```yaml
   base_parameters:
     catalog_name: ${var.catalog_name}
     schema_name: synthea_test  # Use test schema
   ```

2. Trigger job manually:
   - Go to **Workflows** → Synthea Pipeline
   - Click **Run Now**
   - Monitor execution in Spark UI

3. Validate results:
   - Check runtime (should be <5 minutes)
   - Verify SUCCESS files created
   - Validate data quality injection rates

---

## Step 4: Validation Queries

Run these queries after test execution:

### Check Null Injection Rates

```sql
-- encounters.csv null rate in PATIENT column
SELECT
  COUNT(*) as total_rows,
  SUM(CASE WHEN PATIENT IS NULL THEN 1 ELSE 0 END) as null_count,
  ROUND(SUM(CASE WHEN PATIENT IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM {catalog}.{schema}.encounters;

-- Expected: 1-5% null rate
```

### Check Negative Value Injection Rates

```sql
-- encounters.csv negative rate in PAYER_COVERAGE column
SELECT
  COUNT(*) as total_rows,
  SUM(CASE WHEN PAYER_COVERAGE < 0 THEN 1 ELSE 0 END) as negative_count,
  ROUND(SUM(CASE WHEN PAYER_COVERAGE < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_percentage
FROM {catalog}.{schema}.encounters;

-- Expected: 1-5% negative rate
```

### Verify File Processing

```python
# Check all success files exist
test_path = "/Volumes/{catalog}/{schema}/synthetic_files_raw/output/csv_test/"
directories = spark.sql(f"LIST '{test_path}' ").collect()

for directory in directories:
    directory_path = directory[0]
    success_file = directory_path + "/data_quality_output/SUCCESS.txt"
    try:
        content = dbutils.fs.head(success_file)
        print(f"✅ {directory[1]}: {content}")
    except:
        print(f"❌ {directory[1]}: SUCCESS file missing")
```

---

## Step 5: Production Deployment

Once testing is successful:

1. **Update production job configuration**:
   ```bash
   cd bundles/data_engineering
   databricks bundle deploy --target prod
   ```

2. **Monitor first production run**:
   - Watch Spark UI for cluster utilization
   - Check runtime metrics
   - Validate output data quality

3. **Document changes**:
   - Update runbook with new expected runtime
   - Update alert thresholds (from 4 hours → 10 minutes)
   - Document rollback procedure

---

## Step 6: Monitoring Setup

### Create Dashboard Queries

```sql
-- Runtime trend
SELECT
  DATE(start_time) as run_date,
  task_key,
  ROUND(execution_duration / 60000, 2) as duration_minutes
FROM system.lakeflow.task_runs
WHERE task_key = 'inject_bad_data'
  AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY start_time DESC;
```

### Set Up Alerts

**Alert 1: Slow Execution**
- **Condition**: Runtime > 15 minutes
- **Action**: Notify team via Teams/Slack
- **Reason**: Should complete in 2-5 minutes

**Alert 2: Task Failure**
- **Condition**: Task status = FAILED
- **Action**: Page on-call engineer
- **Reason**: Critical data quality step

**Alert 3: Data Quality Issues**
- **Condition**: Null/negative rates outside 1-5% range
- **Action**: Notify data engineering team
- **Reason**: Incorrect injection rates indicate bug

---

## Rollback Procedure

If issues occur in production:

### Immediate Rollback

1. Go to **Workflows** → Synthea Pipeline → **Edit**
2. Update `inject_bad_data` task:
   ```yaml
   notebook_task:
     source: GIT
     notebook_path: notebooks/01-data-generation/2.0-inject-bad-data
   timeout_seconds: 14400  # Restore 4-hour timeout
   ```
3. Click **Save**
4. Trigger rerun of failed task

### Root Cause Analysis

1. Check Spark UI logs for error messages
2. Verify cluster had sufficient resources
3. Check if data volume exceeded expectations
4. Review validation query results

### Fix and Redeploy

1. Address identified issue in optimized code
2. Test with small dataset again
3. Redeploy to production

---

## Performance Comparison

After production deployment, document actual results:

| Metric | Original | Optimized | Improvement |
|--------|----------|-----------|-------------|
| Runtime | 3.5 hours | ___ minutes | ___x faster |
| Cluster CPU | 15% avg | ___% avg | ___x better |
| Cost per run | $8.75 | $___ | ___% savings |
| Success rate | 100% | ___% | - |

---

## Troubleshooting

### Issue: "File not found" error

**Cause**: Notebook path incorrect or not uploaded

**Solution**:
```bash
# Verify notebook exists
databricks workspace ls /Users/<your-user>/synthea_optimization

# Re-upload if needed
databricks workspace import \
  notebooks/synthea_optimization/inject_bad_data_optimized.py \
  /Users/<your-user>/synthea_optimization/inject_bad_data_optimized \
  --language PYTHON \
  --overwrite
```

### Issue: "Out of memory" error

**Cause**: Cluster undersized for data volume

**Solution**: Increase worker count or memory:
```yaml
new_cluster:
  num_workers: 8  # Increase from 4
  node_type_id: "Standard_DS5_v2"  # Larger instance
```

### Issue: Runtime still slow (>15 minutes)

**Cause**: Possible reasons:
- Data volume larger than expected
- Cluster not scaling properly
- Network I/O bottleneck

**Solution**:
1. Check Spark UI → Storage tab for shuffle size
2. Verify cluster autoscaling is enabled
3. Check if coalesce(1) is causing bottleneck:
   ```python
   # If files are very large, increase partition count
   df.repartition(10).write...  # Instead of coalesce(1)
   ```

### Issue: Incorrect null/negative injection rates

**Cause**: Random seed or fraction misconfigured

**Solution**: Verify null_fraction parameter:
```python
# Should be between 0.01 and 0.05
null_fraction = random.uniform(0.01, 0.05)
```

---

## Next Steps

After successful deployment:

1. **Documentation**:
   - Update pipeline documentation with new runtime expectations
   - Document optimized version in team wiki
   - Share performance analysis with stakeholders

2. **Upstream Contribution**:
   - Consider submitting PR to https://github.com/matthew-gigl-db/synthea-on-dbx
   - Include performance analysis and benchmarks
   - Help community benefit from optimization

3. **Further Optimization**:
   - Monitor for additional bottlenecks
   - Consider optimizing other pipeline tasks
   - Apply similar Spark-native patterns elsewhere

---

## Contact

For questions or issues with implementation:
- Review: `/Users/dwtorres/src/work/hlsFHIRDemo/notebooks/synthea_optimization/PERFORMANCE_ANALYSIS.md`
- Check: Optimized notebook code and inline comments
- Debug: Enable Spark SQL logging for detailed execution plans

---

## Summary Checklist

- [ ] Optimized notebook uploaded to workspace
- [ ] Job configuration updated with new notebook path
- [ ] Timeout reduced from 4 hours to 10 minutes
- [ ] Test run completed successfully (<5 minutes)
- [ ] Validation queries show correct injection rates (1-5%)
- [ ] SUCCESS files created for all directories
- [ ] Production deployment completed
- [ ] Monitoring dashboard created
- [ ] Alerts configured for slow execution and failures
- [ ] Team documentation updated
- [ ] Rollback procedure tested and documented
