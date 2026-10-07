# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Performance Registry
# MAGIC %md
# MAGIC ### Lakehouse RT
# MAGIC
# MAGIC **Lakehouse RT** is the real-time / low-latency query and serving layer of the lakehouse — the idea that you can run fast, interactive or operational queries directly on your Delta/Unity Catalog tables (increasingly with Lakebase for OLTP and Real-Time Mode streaming feeding fresh data in), instead of copying data out to a separate real-time store. 
# MAGIC
# MAGIC #### Why it leans hard on managed-table best practices
# MAGIC
# MAGIC Real-time performance is almost entirely a function of how well the underlying table is physically maintained, and the automatic maintenance features only fully kick in on UC managed tables (not external tables, where Databricks doesn't own the storage lifecycle). Specifically:
# MAGIC
# MAGIC * **Predictive Optimization** — auto-runs OPTIMIZE and VACUUM for you. Only available on managed tables. Without it, small files pile up and latency degrades.
# MAGIC * **Liquid Clustering** — keeps data laid out so queries prune to the right files instead of scanning everything; critical when you need sub-second responses.
# MAGIC * **Auto-compaction / optimized writes** — prevents the small-file problem that kills real-time read latency.
# MAGIC * **Deletion vectors + fresh statistics** — let the engine skip data efficiently and plan well.
# MAGIC * **Ownership of the storage path** — managed tables let Databricks garbage-collect, compact, and relocate data safely; external tables put that burden on you and it usually doesn't happen consistently.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Registry
# MAGIC
# MAGIC Use this table to log query performance observations for the reviewed table.
# MAGIC
# MAGIC | Query ID | Target [ms] | DBSQL time [ms] | Lakehouse RT time [ms] | Competitor |
# MAGIC |----------|-------------|-----------------|------------------------|------------|
# MAGIC | query__1 | < 3,000     |     10,709      |         3,145          |    8,200   |
# MAGIC | query__2 | < 3,000     |     12,400      |         2,980          |    9,500   |
# MAGIC | query__3 | < 4,500     |     8,900       |         #####          |    7,100   |
# MAGIC | query__# | < ######    |     ######      |         #####          |    #####   |
# MAGIC | query__# | < ######    |     ######      |         #####          |    #####   |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Table Best Practices Reviewer
# MAGIC
# MAGIC This notebook evaluates a Unity Catalog table against Databricks best practices and provides **automated remediation actions** you can run directly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Quick Start
# MAGIC
# MAGIC 1. Fill in the **widgets** at the top:
# MAGIC    * `catalog` — the catalog name
# MAGIC    * `schema` — the schema name
# MAGIC    * `table_name` — just the table name (or the full `catalog.schema.table` path)
# MAGIC 2. **Run All** cells from top to bottom.
# MAGIC 3. Review the check results and the summary scorecard.
# MAGIC 4. Optionally run the remediation cells (CLUSTER BY AUTO, OPTIMIZE, ANALYZE TABLE) for any failing checks.
# MAGIC 5. Add the comparison sheet on the table at the end
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### What it evaluates
# MAGIC
# MAGIC | # | Check | Severity |
# MAGIC |---|-------|----------|
# MAGIC | 1 | Table type (managed vs external) | FAIL if external |
# MAGIC | 2 | Owner assigned | FAIL if missing |
# MAGIC | 3 | Table comment / description | FAIL if missing |
# MAGIC | 4 | Column comment coverage (≥80%) | WARN if below threshold |
# MAGIC | 5 | Delta format | FAIL if non-Delta |
# MAGIC | 6 | Unity Catalog tags | WARN if none |
# MAGIC | 7 | Column statistics (ANALYZE TABLE) | WARN/FAIL |
# MAGIC | 8 | Primary key constraints | WARN if none |
# MAGIC | 9 | Partitioning or Liquid Clustering | WARN if none |
# MAGIC | 10 | OPTIMIZE & ANALYZE history | FAIL if never run |
# MAGIC | 11 | Row count (non-empty) | FAIL if 0 rows |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Remediation actions included
# MAGIC
# MAGIC * **CLUSTER BY AUTO** — enables automatic Liquid Clustering if no layout strategy exists.
# MAGIC * **OPTIMIZE** — compacts small files and applies clustering layout.
# MAGIC * **ANALYZE TABLE** — computes column-level statistics for the query optimizer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Performance Registry
# MAGIC
# MAGIC At the bottom of the notebook there is a table to manually log query performance benchmarks (DBSQL time, Lakehouse RT time, competitor) for tracking improvements after applying best practices.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameter Widget
# Read widget values
_catalog_param = dbutils.widgets.get("catalog").strip()
_schema_param = dbutils.widgets.get("schema").strip()
_table_param = dbutils.widgets.get("table_name").strip()

# If table_name is already fully qualified, use it directly.
# Otherwise, build from individual catalog / schema / table_name widgets.
if _table_param.count(".") == 2:
    table_name = _table_param
elif _catalog_param and _schema_param and _table_param:
    table_name = f"{_catalog_param}.{_schema_param}.{_table_param}"
else:
    table_name = _table_param  # will fail validation later if incomplete

print("Provide either:")
print("  • A fully qualified name in the table_name widget (catalog.schema.table), OR")
print("  • Individual catalog, schema, and table_name widgets.")
print()
print(f"  catalog    = {_catalog_param or '<not set>'}")
print(f"  schema     = {_schema_param or '<not set>'}")
print(f"  table_name = {_table_param or '<not set>'}")
print(f"  resolved   = {table_name or '<not set>'}")


# COMMAND ----------

# DBTITLE 1,Load Table Metadata (In case of changes this will need to be loaded again)
from pyspark.sql import Row
import pandas as pd

STATUS_LABELS = {
    "PASS": "✅ PASS",
    "WARN": "⚠️ WARN",
    "FAIL": "❌ FAIL",
}

review_results = []

def upsert_check(check_name: str, status: str, note: str) -> None:
    global review_results
    review_results = [row for row in review_results if row["check"] != check_name]
    review_results.append({"check": check_name, "status": status, "note": note})
    print(f"{STATUS_LABELS[status]} | {check_name}: {note}")


def create_temp_view(name: str, rows: list[dict], schema: str) -> None:
    spark.createDataFrame(rows, schema=schema).createOrReplaceTempView(name)


def is_non_empty_text(value) -> bool:
    return value is not None and str(value).strip() not in {"", "null", "none", "[]", "{}"}


def parse_describe_table_extended(rows) -> tuple[list[dict], dict]:
    columns = []
    metadata = {}
    in_metadata = False

    for row in rows:
        col_name = row["col_name"]
        data_type = row["data_type"]
        comment = row["comment"]

        if col_name is None:
            continue

        col_name = str(col_name)
        if col_name.startswith("#"):
            continue
        if col_name.strip() == "":
            in_metadata = True
            continue

        if in_metadata:
            metadata[col_name.rstrip(":").strip()] = data_type
        else:
            columns.append(
                {
                    "column_name": col_name.strip(),
                    "data_type": data_type,
                    "comment": comment,
                }
            )

    return columns, metadata


# table_name is resolved in the Parameter Widget cell above
table_context = {
    "input": table_name,
    "ready": False,
    "error": None,
    "catalog": None,
    "schema": None,
    "table": None,
    "quoted_name": None,
    "detail": {},
    "extended": {},
    "columns": [],
}

create_temp_view(
    "table_review_metadata_summary",
    [{"section": "Input", "item": "table_name", "value": table_name or "<not set>"}],
    "section STRING, item STRING, value STRING",
)
create_temp_view(
    "table_review_columns",
    [],
    "column_name STRING, data_type STRING, comment STRING",
)
create_temp_view(
    "table_review_describe_detail",
    [],
    "placeholder STRING",
)
create_temp_view(
    "table_review_describe_extended",
    [],
    "col_name STRING, data_type STRING, comment STRING",
)

if not table_name:
    print("No table selected yet. Populate the widget and rerun the notebook.")
else:
    parts = table_name.split(".")
    if len(parts) != 3 or any(not part.strip() for part in parts):
        table_context["error"] = "Expected table_name in catalog.schema.table format."
        print(table_context["error"])
    else:
        catalog, schema, table = [part.strip() for part in parts]
        quoted_name = f"`{catalog}`.`{schema}`.`{table}`"
        table_context.update(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "quoted_name": quoted_name,
            }
        )

        try:
            describe_detail_df = spark.sql(f"DESCRIBE DETAIL {quoted_name}")
            describe_extended_df = spark.sql(f"DESCRIBE TABLE EXTENDED {quoted_name}")

            detail_row = describe_detail_df.first().asDict(recursive=True)
            extended_rows = [row.asDict(recursive=True) for row in describe_extended_df.collect()]
            column_rows, extended_metadata = parse_describe_table_extended(extended_rows)

            table_context.update(
                {
                    "ready": True,
                    "detail": detail_row,
                    "extended": extended_metadata,
                    "columns": column_rows,
                }
            )

            describe_detail_df.createOrReplaceTempView("table_review_describe_detail")
            describe_extended_df.createOrReplaceTempView("table_review_describe_extended")
            create_temp_view(
                "table_review_columns",
                column_rows,
                "column_name STRING, data_type STRING, comment STRING",
            )

            metadata_summary_rows = [
                {"section": "Input", "item": "table_name", "value": table_name},
                {"section": "Detail", "item": "type", "value": str(detail_row.get("type"))},
                {"section": "Detail", "item": "format", "value": str(detail_row.get("format"))},
                {"section": "Detail", "item": "location", "value": str(detail_row.get("location"))},
                {"section": "Detail", "item": "description", "value": str(detail_row.get("description"))},
                {"section": "Detail", "item": "partitionColumns", "value": str(detail_row.get("partitionColumns"))},
                {"section": "Detail", "item": "tableFeatures", "value": str(detail_row.get("tableFeatures"))},
                {"section": "Extended", "item": "Owner", "value": str(extended_metadata.get("Owner"))},
                {"section": "Extended", "item": "Type", "value": str(extended_metadata.get("Type"))},
                {"section": "Extended", "item": "Provider", "value": str(extended_metadata.get("Provider"))},
                {"section": "Extended", "item": "Comment", "value": str(extended_metadata.get("Comment"))},
                {"section": "Extended", "item": "Statistics", "value": str(extended_metadata.get("Statistics"))},
                {"section": "Derived", "item": "column_count", "value": str(len(column_rows))},
            ]
            create_temp_view(
                "table_review_metadata_summary",
                metadata_summary_rows,
                "section STRING, item STRING, value STRING",
            )

            print(f"Loaded metadata for {quoted_name}")
            print(f"Detected {len(column_rows)} columns, format={detail_row.get('format')}, type={detail_row.get('type')}")
        except Exception as exc:
            table_context["error"] = str(exc)
            create_temp_view(
                "table_review_metadata_summary",
                [
                    {"section": "Input", "item": "table_name", "value": table_name},
                    {"section": "Error", "item": "message", "value": table_context["error"]},
                ],
                "section STRING, item STRING, value STRING",
            )
            print(f"Unable to load metadata for {quoted_name}: {table_context['error']}")


# COMMAND ----------

# DBTITLE 1,Metadata Snapshot
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_review_metadata_summary
# MAGIC ORDER BY section, item
# MAGIC

# COMMAND ----------

# DBTITLE 1,About: Table Type
# MAGIC %md
# MAGIC ### Checks — Table Type, Data Format & Layout Strategy
# MAGIC
# MAGIC | Check | Why it matters |
# MAGIC |-------|----------------|
# MAGIC | **Table Type** | Managed tables live in UC managed storage with full lifecycle control (drop = delete). External tables miss some UC features. |
# MAGIC | **Data Format** | Delta provides ACID transactions, time travel, schema enforcement, Z-ordering, Liquid Clustering, and Photon optimization. Non-Delta formats miss most of these. |
# MAGIC | **Layout Strategy** | Liquid Clustering (recommended) or partitioning controls physical file layout to minimize I/O on common query predicates. |
# MAGIC
# MAGIC * **PASS** — Best practice followed for that check.
# MAGIC * **WARN / FAIL** — Action needed; see the note for the specific remediation command.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check Table Type
if not table_context.get("ready"):
    upsert_check("Table type", "WARN", "Set a valid table_name widget value to evaluate whether the table is managed.")
    upsert_check("Data format", "WARN", "Set a valid table_name widget value to inspect the storage format.")
    upsert_check("Partitioning or Liquid Clustering", "WARN", "Set a valid table_name widget value to inspect partitioning or clustering.")
else:
    # ── Check 1: Table Type ───────────────────────────────────────────────────
    table_type = str(table_context["detail"].get("type") or table_context["extended"].get("Type") or "").upper()
    if table_type == "MANAGED":
        upsert_check("Table type", "PASS", "The table is Unity Catalog managed.")
    elif table_type:
        upsert_check("Table type", "FAIL", f"The table is {table_type}, not MANAGED.")
    else:
        upsert_check("Table type", "WARN", "Table type could not be determined from the metadata.")

    # ── Check 4: Data Format ──────────────────────────────────────────────────
    data_format = str(table_context["detail"].get("format") or table_context["extended"].get("Provider") or "").lower()
    if data_format == "delta":
        upsert_check("Data format", "PASS", "The table uses Delta format.")
    elif data_format:
        upsert_check("Data format", "FAIL", f"The table format is {data_format}, not Delta.")
    else:
        upsert_check("Data format", "WARN", "Table format could not be determined from the metadata.")

    # ── Check 8: Partitioning or Liquid Clustering ────────────────────────────
    detail = table_context.get("detail", {})
    properties = detail.get("properties") or {}
    partition_columns = detail.get("partitionColumns") or []
    clustering_columns = detail.get("clusteringColumns") or []
    table_features = [str(f).lower() for f in (detail.get("tableFeatures") or [])]
    liquid_enabled = (
        bool(clustering_columns)
        or ("clustering" in table_features)
        or is_non_empty_text(properties.get("delta.clusteringColumns"))
        or str(properties.get("delta.feature.clustering", "")).lower() in {"supported", "enabled", "true"}
    )

    if partition_columns:
        upsert_check("Partitioning or Liquid Clustering", "PASS", f"The table is partitioned by: {', '.join(partition_columns)}.")
    elif liquid_enabled:
        cluster_note = ", ".join(clustering_columns) if clustering_columns else "metadata indicates Liquid Clustering is enabled"
        upsert_check("Partitioning or Liquid Clustering", "PASS", f"The table uses Liquid Clustering ({cluster_note}).")
    else:
        upsert_check("Partitioning or Liquid Clustering", "WARN", "No partition columns or Liquid Clustering metadata were detected.")


# COMMAND ----------

# DBTITLE 1,About: Apply Liquid Clustering AUTO
# MAGIC %md
# MAGIC ### Apply Liquid Clustering (AUTO)
# MAGIC
# MAGIC If the table has no partitioning or clustering strategy, you can enable **Liquid Clustering with automatic column selection** using:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.table CLUSTER BY AUTO
# MAGIC ```
# MAGIC
# MAGIC Databricks will automatically determine the best clustering columns based on query patterns and data characteristics. This is the simplest way to get optimized file layout without manual column selection.
# MAGIC
# MAGIC > ⚠️ This modifies the table’s metadata. Existing data will be reorganized on the next `OPTIMIZE` run (or automatically if Predictive Optimization is enabled).
# MAGIC

# COMMAND ----------

# DBTITLE 1,Apply CLUSTER BY AUTO
if not table_context.get("ready"):
    print("No table loaded — set the widget values and run the metadata cell first.")
else:
    # Check if already clustered or partitioned
    detail = table_context.get("detail", {})
    partition_columns = detail.get("partitionColumns") or []
    clustering_columns = detail.get("clusteringColumns") or []

    if clustering_columns:
        print(f"ℹ️  Table already uses Liquid Clustering on: {', '.join(clustering_columns)}")
        print("   No action taken. Remove this clustering first if you want to switch to AUTO.")
    elif partition_columns:
        print(f"ℹ️  Table is partitioned by: {', '.join(partition_columns)}")
        print("   Partitioned tables cannot use CLUSTER BY. Drop partitions first if you want to migrate.")
    else:
        print(f"Applying CLUSTER BY AUTO to {table_context['quoted_name']}...")
        spark.sql(f"ALTER TABLE {table_context['quoted_name']} CLUSTER BY AUTO")
        print("✅ Done. Liquid Clustering (AUTO) is now enabled.")
        print("   Data will be reorganized on the next OPTIMIZE run.")

# COMMAND ----------

# DBTITLE 1,About: Optimize and Analyze History
# MAGIC %md
# MAGIC ### Check — OPTIMIZE & ANALYZE History
# MAGIC
# MAGIC Regularly running **OPTIMIZE** compacts small files into larger, more efficient ones and applies Z-ordering or Liquid Clustering layout. Running **ANALYZE TABLE** populates column-level statistics that help the query optimizer make better decisions (join strategies, broadcast thresholds, filter selectivity).
# MAGIC
# MAGIC This check inspects `DESCRIBE HISTORY` for evidence that these maintenance operations have been applied.
# MAGIC
# MAGIC * **PASS** — Both OPTIMIZE and ANALYZE have been run at least once.
# MAGIC * **WARN** — Only one of the two has been detected, or neither has been run recently.
# MAGIC * **FAIL** — Neither operation has ever been applied to this table.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check Optimize and Analyze History
check_name = "OPTIMIZE & ANALYZE history"

if not table_context.get("ready"):
    upsert_check(check_name, "WARN", "Set a valid table_name widget value to inspect maintenance history.")
else:
    try:
        history_df = spark.sql(f"DESCRIBE HISTORY {table_context['quoted_name']}")
        history_rows = [row.asDict(recursive=True) for row in history_df.collect()]

        optimize_runs = [row for row in history_rows if str(row.get("operation", "")).upper() == "OPTIMIZE"]
        analyze_runs = [
            row for row in history_rows
            if "ANALYZE" in str(row.get("operation", "")).upper()
            or "COMPUTE STATS" in str(row.get("operation", "")).upper()
        ]

        has_optimize = len(optimize_runs) > 0
        has_analyze = len(analyze_runs) > 0

        # Build a human-readable summary
        details = []
        if has_optimize:
            latest_opt = optimize_runs[0]  # history is ordered newest-first
            details.append(f"OPTIMIZE last run: {latest_opt.get('timestamp', 'unknown')}")
        if has_analyze:
            latest_an = analyze_runs[0]
            details.append(f"ANALYZE last run: {latest_an.get('timestamp', 'unknown')}")

        if has_optimize and has_analyze:
            upsert_check(check_name, "PASS", f"Both operations detected in history. {'; '.join(details)}.")
        elif has_optimize or has_analyze:
            missing = "ANALYZE TABLE" if not has_analyze else "OPTIMIZE"
            upsert_check(
                check_name, "WARN",
                f"Only {'OPTIMIZE' if has_optimize else 'ANALYZE'} found in history. "
                f"Consider also running {missing}. {'; '.join(details)}."
            )
        else:
            upsert_check(
                check_name, "FAIL",
                "Neither OPTIMIZE nor ANALYZE TABLE found in the table history. "
                f"Run: OPTIMIZE {table_context['quoted_name']}; "
                f"ANALYZE TABLE {table_context['quoted_name']} COMPUTE STATISTICS FOR ALL COLUMNS;"
            )
    except Exception as exc:
        upsert_check(check_name, "WARN", f"Unable to inspect table history: {exc}")


# COMMAND ----------

# DBTITLE 1,About: Run Optimize and Analyze
# MAGIC %md
# MAGIC ### Run OPTIMIZE & ANALYZE TABLE
# MAGIC
# MAGIC Use the cells below to **execute** OPTIMIZE and ANALYZE TABLE on the target table. These are separate cells so you can run them independently.
# MAGIC
# MAGIC * **OPTIMIZE** — Compacts small files, applies Z-ordering or Liquid Clustering layout. Safe to run repeatedly; no data is changed.
# MAGIC * **ANALYZE TABLE** — Computes column-level statistics (min, max, null count, distinct count) used by the query optimizer.
# MAGIC
# MAGIC > ⚠️ These operations may take time on large tables. They are safe (read-only from a data perspective) but will consume compute resources.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Run ANALYZE TABLE
if not table_context.get("ready"):
    print("No table loaded — set the widget values and run the metadata cell first.")
else:
    print(f"Running ANALYZE TABLE on {table_context['quoted_name']}...")
    result = spark.sql(f"ANALYZE TABLE {table_context['quoted_name']} COMPUTE STATISTICS FOR ALL COLUMNS")
    display(result)
    print("Done.")

# COMMAND ----------

# DBTITLE 1,Run OPTIMIZE
if not table_context.get("ready"):
    print("No table loaded — set the widget values and run the metadata cell first.")
else:
    print(f"Running OPTIMIZE on {table_context['quoted_name']}...")
    result = spark.sql(f"OPTIMIZE {table_context['quoted_name']}")
    display(result)
    print("Done.")