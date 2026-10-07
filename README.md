# Table Best Practices Reviewer

A Databricks notebook that checks a Unity Catalog table against Databricks best practices and includes remediation cells you can run directly.

## Checks

| Check | Result |
|-------|--------|
| Table type (managed vs external) | FAIL if not MANAGED |
| Data format | FAIL if not Delta |
| Partitioning or Liquid Clustering | WARN if neither is set |
| OPTIMIZE & ANALYZE history (`DESCRIBE HISTORY`) | FAIL if neither has run, WARN if only one has |

## Remediation cells

- `ALTER TABLE ... CLUSTER BY AUTO`: turns on automatic Liquid Clustering when the table has no layout strategy
- `OPTIMIZE`: compacts small files and applies the clustering layout
- `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`: computes column statistics for the optimizer

The notebook also has a **Performance Registry** table for recording query timings (DBSQL vs Lakehouse RT vs competitor) before and after you apply the fixes. The numbers in it are only examples.

## Usage

1. Import `table_best_practices_reviewer.py` into your workspace (**Workspace → Import**).
2. Create the three text widgets the notebook reads: `catalog`, `schema`, `table_name`. For example, add this cell at the top:
   ```python
   dbutils.widgets.text("catalog", "")
   dbutils.widgets.text("schema", "")
   dbutils.widgets.text("table_name", "")
   ```
   `table_name` also accepts a fully qualified `catalog.schema.table`.
3. Run all cells and review the PASS / WARN / FAIL output.

> The remediation cells change table metadata and use compute. Read them before running against production tables.
