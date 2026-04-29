# ETL Data Quality Framework

> Python · PySpark · Azure SQL · Azure Logic Apps  
> Production-grade reusable data quality library — row reconciliation, null auditing, referential integrity checks, and pipeline alerting.

---

## Business Context

At Cognizant, working on Pearson's K12 North America data migration (500K+ student credential records across 8 schema versions), data quality issues weren't caught until they hit production — causing downstream reporting failures that affected operational teams. This framework implements the automated quality gate pattern that catches those issues before data reaches the Gold layer.

---

## What This Does

Every data pipeline has the same quality problems: unexpected nulls, row count mismatches between source and target, referential integrity breaks, and schema drift. This library gives you a configurable, reusable quality gate that runs post every load and **fails the pipeline before bad data reaches consumers**.

---

## Repository Structure

```
etl-data-quality-framework/
├── dq/
│   ├── __init__.py
│   ├── checks.py              # Core quality check functions
│   ├── reconciliation.py      # Source vs target row reconciliation
│   ├── schema_validator.py    # Schema drift detection
│   └── alerting.py            # Azure Logic Apps alert integration
├── sql/
│   ├── audit_table_ddl.sql    # Quality results audit table
│   └── quality_dashboard.sql  # Power BI-ready quality trend view
├── config/
│   └── quality_config.json    # Thresholds per table
├── tests/
│   ├── test_checks.py
│   ├── test_reconciliation.py
│   └── test_schema_validator.py
├── examples/
│   ├── pearson_credentials_pipeline.py   # Full example: Pearson pattern
│   └── retail_pipeline_integration.py    # Full example: retail pattern
└── README.md
```

---

## Core Quality Checks

```python
# dq/checks.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Reusable data quality gate for PySpark pipelines.
    Runs configurable checks and fails fast before Gold layer load.
    """

    def __init__(self, config: dict):
        self.config = config
        self.results = []

    def check_row_count(self, source_df: DataFrame, target_df: DataFrame,
                        table_name: str, tolerance_pct: float = 0.0) -> bool:
        """
        Source and target must have matching row counts within tolerance.
        Catches silent truncation, filter errors, and load failures.
        """
        source_count = source_df.count()
        target_count = target_df.count()

        if source_count == 0:
            logger.warning(f"[{table_name}] Source is empty — possible upstream failure")
            self._log_result(table_name, "row_count", False,
                             f"Source empty: {source_count}")
            return False

        variance_pct = abs(source_count - target_count) / source_count * 100

        passed = variance_pct <= tolerance_pct
        self._log_result(table_name, "row_count", passed,
                         f"Source: {source_count:,} | Target: {target_count:,} | "
                         f"Variance: {variance_pct:.2f}% (threshold: {tolerance_pct}%)")
        return passed

    def check_null_percentage(self, df: DataFrame, table_name: str,
                               thresholds: Dict[str, float]) -> bool:
        """
        Null percentage per column must be within configured thresholds.
        Catches upstream data feed degradation early.
        """
        all_passed = True
        for col_name, max_null_pct in thresholds.items():
            if col_name not in df.columns:
                continue
            total = df.count()
            null_count = df.filter(F.col(col_name).isNull()).count()
            actual_pct = (null_count / total * 100) if total > 0 else 0

            passed = actual_pct <= max_null_pct
            if not passed:
                all_passed = False
            self._log_result(table_name, f"null_pct_{col_name}", passed,
                             f"{col_name}: {actual_pct:.2f}% nulls (max: {max_null_pct}%)")
        return all_passed

    def check_referential_integrity(self, fact_df: DataFrame, dim_df: DataFrame,
                                     fact_key: str, dim_key: str,
                                     table_name: str) -> bool:
        """
        All foreign keys in fact table must exist in dimension table.
        Catches orphaned records before they corrupt downstream aggregations.
        """
        orphans = (fact_df
            .select(fact_key)
            .join(dim_df.select(dim_key),
                  fact_df[fact_key] == dim_df[dim_key], how="left_anti"))

        orphan_count = orphans.count()
        passed = orphan_count == 0
        self._log_result(table_name, f"ref_integrity_{fact_key}", passed,
                         f"Orphaned {fact_key} values: {orphan_count:,}")
        return passed

    def check_no_duplicates(self, df: DataFrame, key_cols: List[str],
                             table_name: str) -> bool:
        """
        Business key columns must be unique (no duplicate primary keys).
        """
        total = df.count()
        distinct = df.dropDuplicates(key_cols).count()
        dupe_count = total - distinct

        passed = dupe_count == 0
        self._log_result(table_name, "duplicates", passed,
                         f"Duplicate records on {key_cols}: {dupe_count:,}")
        return passed

    def check_value_ranges(self, df: DataFrame, table_name: str,
                            range_checks: Dict[str, tuple]) -> bool:
        """
        Numeric columns must fall within expected ranges.
        Catches data type errors, currency conversion bugs, and feed corruption.
        """
        all_passed = True
        for col_name, (min_val, max_val) in range_checks.items():
            out_of_range = df.filter(
                (F.col(col_name) < min_val) | (F.col(col_name) > max_val)
            ).count()
            passed = out_of_range == 0
            if not passed:
                all_passed = False
            self._log_result(table_name, f"range_{col_name}", passed,
                             f"{col_name} out of [{min_val}, {max_val}]: {out_of_range:,} records")
        return all_passed

    def run_all(self) -> bool:
        """Returns True if all registered checks passed. Fails fast if any failed."""
        failed = [r for r in self.results if not r["passed"]]
        if failed:
            failure_summary = "\n".join([f"  ✗ {r['check']}: {r['detail']}" for r in failed])
            raise ValueError(f"Data quality FAILED — {len(failed)} check(s):\n{failure_summary}")
        logger.info(f"All {len(self.results)} quality checks passed ✓")
        return True

    def _log_result(self, table: str, check: str, passed: bool, detail: str):
        status = "✓" if passed else "✗"
        logger.info(f"[{table}] [{status}] {check}: {detail}")
        self.results.append({"table": table, "check": check,
                              "passed": passed, "detail": detail})
```

---

## Row Reconciliation

```python
# dq/reconciliation.py
def reconcile_source_to_target(source_df: DataFrame, target_df: DataFrame,
                                 key_col: str, value_cols: List[str],
                                 table_name: str) -> dict:
    """
    Row-level reconciliation: compare source vs target on key columns.
    Returns summary of matched, missing, and mismatched records.

    This is the pattern that caught 3 critical data discrepancies in the
    Pearson K12 migration before they reached production.
    """
    source_keyed = source_df.select([key_col] + value_cols)
    target_keyed = target_df.select([key_col] + value_cols)

    # In source but not in target
    missing_in_target = source_keyed.join(
        target_keyed, on=key_col, how="left_anti").count()

    # In target but not in source
    extra_in_target = target_keyed.join(
        source_keyed, on=key_col, how="left_anti").count()

    # Matched on key but values differ
    joined = source_keyed.alias("s").join(
        target_keyed.alias("t"), on=key_col, how="inner")

    mismatch_condition = " OR ".join(
        [f"s.{c} != t.{c}" for c in value_cols])
    mismatched = joined.filter(mismatch_condition).count()

    total = source_keyed.count()
    matched = total - missing_in_target - mismatched

    summary = {
        "table":              table_name,
        "source_rows":        total,
        "matched":            matched,
        "missing_in_target":  missing_in_target,
        "extra_in_target":    extra_in_target,
        "mismatched_values":  mismatched,
        "match_rate_pct":     round(matched / total * 100, 2) if total > 0 else 0
    }

    logger.info(f"Reconciliation [{table_name}]: {summary}")
    return summary
```

---

## Full Pipeline Integration Example

```python
# examples/pearson_credentials_pipeline.py
"""
Mirrors the data quality pattern used in Pearson K12 North America migration.
500K+ student credential records across 8 schema versions.
"""
from dq.checks import DataQualityChecker
from dq.reconciliation import reconcile_source_to_target

def run_pipeline(source_df, silver_df, gold_df, fact_sales_df, dim_student_df):

    # --- Silver layer quality gate ---
    silver_checker = DataQualityChecker(config={})

    silver_checker.check_row_count(source_df, silver_df, "student_credentials",
                                    tolerance_pct=0.0)  # Zero loss expected

    silver_checker.check_null_percentage(silver_df, "student_credentials", {
        "student_id":    0.0,   # Must never be null
        "credential_id": 0.0,   # Must never be null
        "issue_date":    0.0,   # Must never be null
        "student_email": 2.0,   # Up to 2% allowed (legacy records)
    })

    silver_checker.check_no_duplicates(silver_df,
        key_cols=["student_id", "credential_id"],
        table_name="student_credentials")

    silver_checker.check_value_ranges(silver_df, "student_credentials", {
        "score": (0, 100),       # Scores must be 0–100
        "credit_hours": (0, 24), # Max 24 credit hours per course
    })

    silver_checker.run_all()  # Raises if any check failed — pipeline stops here

    # --- Gold layer referential integrity ---
    gold_checker = DataQualityChecker(config={})

    gold_checker.check_referential_integrity(
        fact_sales_df, dim_student_df,
        fact_key="student_sk", dim_key="student_sk",
        table_name="fact_enrollments")

    gold_checker.run_all()

    # --- Full reconciliation report ---
    recon = reconcile_source_to_target(
        source_df, gold_df,
        key_col="student_id",
        value_cols=["credential_id", "issue_date", "status"],
        table_name="student_credentials")

    if recon["match_rate_pct"] < 99.9:
        raise ValueError(f"Reconciliation below threshold: {recon['match_rate_pct']}%")

    print("Pipeline complete — all quality checks passed ✓")
```

---

## SQL Audit Table

```sql
-- sql/audit_table_ddl.sql
CREATE TABLE dbo.dq_audit_log (
    audit_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_date      DATETIME2     NOT NULL DEFAULT GETDATE(),
    pipeline_name NVARCHAR(100) NOT NULL,
    table_name    NVARCHAR(100) NOT NULL,
    check_name    NVARCHAR(100) NOT NULL,
    passed        BIT           NOT NULL,
    detail        NVARCHAR(500) NULL,
    row_count     BIGINT        NULL
);

-- Quality trend view for Power BI monitoring dashboard
CREATE VIEW dbo.vw_dq_trend AS
SELECT
    CAST(run_date AS DATE)  AS run_date,
    pipeline_name,
    table_name,
    COUNT(*)                AS total_checks,
    SUM(CAST(passed AS INT))AS passed_checks,
    COUNT(*) - SUM(CAST(passed AS INT)) AS failed_checks,
    CAST(100.0 * SUM(CAST(passed AS INT)) / COUNT(*) AS DECIMAL(5,2)) AS pass_rate_pct
FROM dbo.dq_audit_log
GROUP BY CAST(run_date AS DATE), pipeline_name, table_name;
```

---

## Running Tests

```bash
git clone https://github.com/SiriChandanaDhurjati/etl-data-quality-framework.git
cd etl-data-quality-framework
pip install pyspark pytest
python -m pytest tests/ -v
```

---

## Author

**Siri Chandana Dhurjati** · [github.com/SiriChandanaDhurjati](https://github.com/SiriChandanaDhurjati)  
Inspired by production data quality patterns used at Cognizant on Pearson's K12 North America data migration.
