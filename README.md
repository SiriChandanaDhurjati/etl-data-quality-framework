# ETL Data Quality Framework

A reusable Python/PySpark data quality library that acts as a gate between Silver and Gold layers in a medallion architecture pipeline. Every load runs row-count reconciliation, null-percentage thresholds, referential integrity checks, and schema validation before data reaches the Gold layer. Failures are logged to an SQL audit table and trigger Azure Logic Apps alerts — data that doesn't pass doesn't move.

Built from patterns used in production at scale: the reconciliation logic in this library caught 3 critical data discrepancies before production release on a 500K+ student credential dataset, preventing downstream reporting errors that would have been invisible until they surfaced in a business report.

---

## The Problem

Data quality failures in ETL pipelines are quiet. A column that started allowing nulls. A foreign key that references a record that no longer exists. A source system that doubled a row count due to a join bug upstream. These issues don't throw errors — they pass silently through the pipeline and surface weeks later as "the numbers don't match" conversations in a board report.

The standard fix — manual spot-checks before each release — doesn't scale. It catches some things and misses others, it's inconsistent across engineers, and it leaves no audit trail. What's needed is a framework that runs the same checks every time, fails fast before bad data reaches the reporting layer, and logs everything so you can see exactly what failed, when, and why.

This library is that framework.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  Silver Layer load completes                                 │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  DQ Framework runs 4 check types                            │
│                                                             │
│  1. Row Reconciliation    — source count == target count?   │
│  2. Null Threshold        — nulls within acceptable limit?  │
│  3. Referential Integrity — all FK values exist in dim?     │
│  4. Schema Validation     — columns, types, not drifted?    │
└─────────────────────┬───────────────────────────────────────┘
                      │
          ┌───────────┴───────────┐
          │ All checks pass       │ Any check fails
          ▼                       ▼
┌──────────────────┐   ┌──────────────────────────────────────┐
│  Proceed to Gold │   │  Pipeline halts                      │
│  layer load      │   │  Result logged to SQL audit table    │
└──────────────────┘   │  Azure Logic Apps alert fires        │
                       │  Gold layer untouched                │
                       └──────────────────────────────────────┘
```

---

## Check Types

### 1. Row Reconciliation

Compares the row count extracted from the source system against what landed in the target table. Catches truncation bugs, failed inserts, and upstream join explosions.

```python
from dq_framework import RowReconciliationCheck

check = RowReconciliationCheck(
    source_count=5_243_891,       # rows extracted from source
    target_table="silver.pos_transactions",
    tolerance_pct=0.0             # zero tolerance for credential data; 0.5 for analytics
)

result = check.run(spark)
# Result: PASS | FAIL | WARNING
# If FAIL: logs source_count, target_count, delta, and pct_difference to audit table
```

**When to use tolerance > 0:** For analytics pipelines where a small number of late-arriving records is expected and acceptable (e.g. web events that take minutes to flush). For credential or financial data, tolerance should be 0.0 — every row must be accounted for.

---

### 2. Null Threshold

Checks the null percentage for specified columns against configurable thresholds. Raises a FAIL if a critical column (like a primary key or required foreign key) contains any nulls. Raises a WARNING if an optional column exceeds its allowed null rate.

```python
from dq_framework import NullThresholdCheck

check = NullThresholdCheck(
    table="silver.student_credentials",
    column_thresholds={
        "student_id":        {"max_null_pct": 0.0,  "severity": "FAIL"},    # never null
        "credential_type":   {"max_null_pct": 0.0,  "severity": "FAIL"},    # never null
        "issue_date":        {"max_null_pct": 0.02, "severity": "WARNING"},  # 2% tolerance
        "middle_name":       {"max_null_pct": 0.40, "severity": "WARNING"},  # optional field
    }
)

result = check.run(spark)
```

---

### 3. Referential Integrity

Verifies that every foreign key value in a fact or staging table has a corresponding record in the referenced dimension. Catches orphaned records that would cause silent data loss when joining in reporting.

```python
from dq_framework import ReferentialIntegrityCheck

check = ReferentialIntegrityCheck(
    fact_table="silver.pos_transactions",
    fact_column="product_id",
    dim_table="gold.dim_product",
    dim_column="product_id",
    allow_orphan_pct=0.0   # 0 = hard fail on any orphan; >0 = threshold-based
)

result = check.run(spark)
# If FAIL: logs the orphaned values sample (up to 100 rows) to audit table
```

---

### 4. Schema Validation

Compares the actual schema of the loaded table against the expected schema definition. Catches schema drift — when a source system adds, drops, or renames a column — before it corrupts downstream transformations.

```python
from dq_framework import SchemaValidationCheck

expected_schema = {
    "transaction_id":    "string",
    "product_id":        "string",
    "store_id":          "string",
    "transaction_date":  "timestamp",
    "quantity":          "integer",
    "unit_price":        "decimal(10,2)",
    "total_amount":      "decimal(12,2)"
}

check = SchemaValidationCheck(
    table="silver.pos_transactions",
    expected_schema=expected_schema,
    allow_extra_columns=False  # True for additive drift tolerance
)

result = check.run(spark)
```

---

## Running a Full Quality Gate

The `DQGate` orchestrates all checks and produces a single PASS/FAIL result. If any check with severity FAIL fails, the gate fails and the pipeline halts before the Gold load.

```python
from dq_framework import DQGate, RowReconciliationCheck, NullThresholdCheck
from dq_framework import ReferentialIntegrityCheck, SchemaValidationCheck

gate = DQGate(
    pipeline_run_id="adf_run_20241115_001",
    layer_transition="silver_to_gold",
    audit_table="dq_audit.check_results",   # SQL table where all results are logged
    alert_on_fail=True                       # triggers Azure Logic Apps webhook
)

gate.add_check(RowReconciliationCheck(source_count=5_243_891, target_table="silver.pos_transactions"))
gate.add_check(NullThresholdCheck(table="silver.pos_transactions", column_thresholds={...}))
gate.add_check(ReferentialIntegrityCheck(fact_table="silver.pos_transactions", ...))
gate.add_check(SchemaValidationCheck(table="silver.pos_transactions", expected_schema={...}))

gate_result = gate.run(spark)

if gate_result.status == "FAIL":
    raise Exception(f"DQ gate failed. {gate_result.failed_check_count} check(s) failed. "
                    f"See audit table for details: run_id={gate_result.pipeline_run_id}")

# If we reach here, all checks passed — proceed to Gold load
print(f"DQ gate passed. {gate_result.passed_check_count} checks passed.")
```

---

## Audit Table Output

Every check result is written to the SQL audit table regardless of pass/fail. This creates a full history of data quality over time — you can see when a column started drifting toward nulls before it becomes a hard failure.

```
┌──────────────────────┬────────────────────────┬──────────────┬───────────┬────────────────────────────────────────────┐
│ pipeline_run_id      │ check_type             │ table_name   │ status    │ detail                                     │
├──────────────────────┼────────────────────────┼──────────────┼───────────┼────────────────────────────────────────────┤
│ adf_run_20241115_001 │ ROW_RECONCILIATION     │ silver.pos_  │ PASS      │ source=5243891, target=5243891, delta=0    │
│ adf_run_20241115_001 │ NULL_THRESHOLD         │ silver.pos_  │ WARNING   │ issue_date null_pct=1.8% (threshold=2.0%)  │
│ adf_run_20241115_001 │ REFERENTIAL_INTEGRITY  │ silver.pos_  │ FAIL      │ 142 orphaned product_ids. Sample: [P9921,  │
│ adf_run_20241115_001 │ SCHEMA_VALIDATION      │ silver.pos_  │ PASS      │ Schema matches expected. 7/7 columns valid  │
└──────────────────────┴────────────────────────┴──────────────┴───────────┴────────────────────────────────────────────┘
```

---

## SQL Audit Table Schema

```sql
CREATE TABLE dq_audit.check_results (
    id                  BIGINT IDENTITY PRIMARY KEY,
    pipeline_run_id     VARCHAR(100)    NOT NULL,
    check_type          VARCHAR(50)     NOT NULL,   -- ROW_RECONCILIATION | NULL_THRESHOLD | REFERENTIAL_INTEGRITY | SCHEMA_VALIDATION
    layer_transition    VARCHAR(50)     NOT NULL,   -- bronze_to_silver | silver_to_gold
    table_name          VARCHAR(200)    NOT NULL,
    column_name         VARCHAR(100)    NULL,       -- populated for column-level checks
    status              VARCHAR(10)     NOT NULL,   -- PASS | FAIL | WARNING
    severity            VARCHAR(10)     NOT NULL,   -- FAIL | WARNING
    detail              NVARCHAR(MAX)   NULL,       -- human-readable result detail
    expected_value      VARCHAR(500)    NULL,       -- e.g. expected row count
    actual_value        VARCHAR(500)    NULL,       -- e.g. actual row count
    run_timestamp       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    pipeline_stage      VARCHAR(100)    NULL        -- ADF pipeline name / Databricks job
);

CREATE INDEX idx_dq_run_id ON dq_audit.check_results (pipeline_run_id);
CREATE INDEX idx_dq_status  ON dq_audit.check_results (status, run_timestamp);
```

---

## Repository Structure

```
etl-data-quality-framework/
│
├── dq_framework/                     # Core library
│   ├── __init__.py
│   ├── gate.py                       # DQGate orchestrator
│   ├── checks/
│   │   ├── __init__.py
│   │   ├── row_reconciliation.py     # Source vs target count check
│   │   ├── null_threshold.py         # Per-column null percentage check
│   │   ├── referential_integrity.py  # FK → dim existence check
│   │   └── schema_validation.py      # Schema drift detection
│   ├── models/
│   │   ├── check_result.py           # CheckResult dataclass
│   │   └── gate_result.py            # GateResult dataclass
│   └── audit/
│       ├── sql_writer.py             # Writes results to SQL audit table
│       └── alert.py                  # Azure Logic Apps webhook trigger
│
├── sql/
│   ├── create_audit_table.sql        # Audit table DDL
│   └── audit_queries.sql             # Useful monitoring queries
│
├── examples/
│   ├── pos_pipeline_gate.py          # Full gate example: POS transactions
│   └── credential_pipeline_gate.py   # Full gate example: student credentials
│
├── tests/
│   ├── test_row_reconciliation.py
│   ├── test_null_threshold.py
│   ├── test_referential_integrity.py
│   └── test_schema_validation.py
│
└── docs/
    └── check_types.md
```

---

## Running Tests

```bash
pip install pyspark pytest pytest-cov

pytest tests/ -v --cov=dq_framework --cov-report=term-missing
```

---

## Why This Matters

Most data quality tooling is applied after data reaches the reporting layer — data observability platforms, BI-level anomaly detection. This framework is different: it's a **gate** that sits inside the pipeline. Bad data never reaches Gold. It's cheaper to catch a schema drift in Silver than to explain to a finance team why their P&L report is wrong.

The check types here — row reconciliation, null thresholds, referential integrity, schema validation — are the four failure modes that cause the majority of silent ETL bugs in production. They're not glamorous. They're also what separates a pipeline that gets trusted from one that gets blamed.

---

*Built to demonstrate production data quality engineering: fail-fast gates between pipeline layers, full audit trail, configurable severity thresholds, and Azure Logic Apps alerting integration.*
