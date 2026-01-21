# Databricks notebook source
# MAGIC %md
# MAGIC # Unused Tables Report
# MAGIC
# MAGIC This notebook scans a Unity Catalog to identify tables that haven't been accessed in a long time.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Enter the **Catalog Name** in the widget.
# MAGIC 2. Select whether to **Fetch Table Sizes**. Note: Fetching sizes runs `DESCRIBE DETAIL` on every table, which can be slow for catalogs with thousands of tables.
# MAGIC 3. Run all cells.
# MAGIC 4. **Export to PDF**: Go to `File` -> `Export` -> `HTML` (then print the HTML to PDF) or `PDF`.

# COMMAND ----------

import os
import sys
from pyspark.sql.functions import col

# Setup path to import 'src' module
# In Databricks Git Folders, the root is usually the parent of notebooks/
try:
    current_dir = os.getcwd()
    # If we are in 'notebooks', the parent is the repo root
    if os.path.basename(current_dir) == 'notebooks':
        repo_root = os.path.dirname(current_dir)
        if repo_root not in sys.path:
            sys.path.append(repo_root)
    else:
        # Fallback or if structure is different
        sys.path.append(os.path.abspath(os.path.join(current_dir, '..')))
except Exception as e:
    print(f"Warning: Path setup might need manual adjustment. Error: {e}")

from src.analyzer import TableUsageAnalyzer
from src.visualizer import plot_days_since_access, plot_unused_vs_used_counts

# COMMAND ----------

# Initialize Widgets
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.dropdown("fetch_sizes", "No", ["Yes", "No"], "Fetch Table Sizes (Slow)")

catalog_name = dbutils.widgets.get("catalog_name")
fetch_sizes = dbutils.widgets.get("fetch_sizes") == "Yes"

print(f"Analyzing catalog: {catalog_name}")
print(f"Fetch sizes: {fetch_sizes}")

# COMMAND ----------

# Run Analysis
analyzer = TableUsageAnalyzer(spark)
report_df = analyzer.generate_report_data(catalog_name)

if fetch_sizes:
    print("Fetching table sizes... This may take time.")
    report_df = analyzer.enrich_with_size(report_df)

# Cache the result
report_df.cache()

# Trigger action
count = report_df.count()
print(f"Found {count} tables in catalog '{catalog_name}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Convert to Pandas for plotting
pdf = report_df.toPandas()

# Plot 1: Used vs Unused
plot_unused_vs_used_counts(pdf)

# COMMAND ----------

# Plot 2: Distribution of Days Since Last Access
plot_days_since_access(pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Data
# MAGIC
# MAGIC The table below lists all tables, sorted by `days_since_last_access` (descending).

# COMMAND ----------

display(report_df.orderBy(col("days_since_last_access").desc_nulls_first()))
