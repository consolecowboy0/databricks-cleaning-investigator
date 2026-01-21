from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, current_date, datediff, when
from pyspark.sql import DataFrame

class TableUsageAnalyzer:
    """
    Analyzes table usage within a specified Unity Catalog.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_table_metadata(self, catalog_name: str) -> DataFrame:
        """
        Retrieves metadata for all tables in the specified catalog.

        Args:
            catalog_name (str): The name of the catalog to scan.

        Returns:
            DataFrame: A DataFrame containing table details (schema, name, owner, etc.).
        """
        # We query system.information_schema.tables
        # We filter out information_schema itself to focus on user tables
        query = f"""
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_owner,
                comment as table_comment,
                table_type
            FROM system.information_schema.tables
            WHERE table_catalog = '{catalog_name}'
            AND table_schema != 'information_schema'
        """
        return self.spark.sql(query)

    def get_last_access_date(self, catalog_name: str) -> DataFrame:
        """
        Retrieves the last access date for tables in the specified catalog
        from the system.access.audit table.

        Args:
            catalog_name (str): The name of the catalog to scan.

        Returns:
            DataFrame: A DataFrame with table identifiers and the last access date.
        """
        # We query the audit log for 'generateTemporaryTableCredential' actions.
        # This action is a reliable indicator that a table was accessed for a query.
        # The table's full path is available directly in the request parameters.
        query = f"""
            SELECT
                request_params.table_catalog as table_catalog,
                request_params.table_schema as table_schema,
                request_params.table_name as table_name,
                max(event_time) as last_accessed_date
            FROM system.access.audit
            WHERE action_name = 'generateTemporaryTableCredential'
              AND request_params.table_catalog = '{catalog_name}'
            GROUP BY 1, 2, 3
        """
        return self.spark.sql(query)

    def generate_report_data(self, catalog_name: str) -> DataFrame:
        """
        Combines metadata and usage data to generate a comprehensive report.
        Calculates 'days_since_last_access'.

        Args:
            catalog_name (str): The name of the catalog to analyze.

        Returns:
            DataFrame: A DataFrame with table metadata, last access date, and days unused.
        """
        metadata_df = self.get_table_metadata(catalog_name)
        usage_df = self.get_last_access_date(catalog_name)

        # Join metadata with usage (Left Join to keep unused tables)
        # We join on schema and name (catalog is already filtered)
        joined_df = metadata_df.alias("m").join(
            usage_df.alias("u"),
            (col("m.table_catalog") == col("u.table_catalog")) &
            (col("m.table_schema") == col("u.table_schema")) &
            (col("m.table_name") == col("u.table_name")),
            "left"
        ).select(
            col("m.table_catalog"),
            col("m.table_schema"),
            col("m.table_name"),
            col("m.table_owner"),
            col("m.table_type"),
            col("u.last_accessed_date")
        )

        # Calculate days since last access
        # If last_accessed_date is NULL, it means never accessed (in the retained history)
        result_df = joined_df.withColumn(
            "days_since_last_access",
            datediff(current_date(), col("last_accessed_date"))
        ).withColumn(
            "usage_status",
            when(col("last_accessed_date").isNull(), "Never Accessed")
            .otherwise("Accessed")
        )

        return result_df

    def enrich_with_size(self, df: DataFrame) -> DataFrame:
        """
        Enriches the given DataFrame with table size information using DESCRIBE DETAIL.

        Warning: This is a slow operation as it iterates through each table.
        It is recommended to run this on a filtered DataFrame or small catalog.

        Args:
            df (DataFrame): The DataFrame containing table info (must have table_catalog, table_schema, table_name).

        Returns:
            DataFrame: The input DataFrame with an added 'size_in_bytes' column.
        """
        # Collect relevant columns to the driver
        # We limit the selection to unique tables to avoid redundant queries if the input is somehow duplicated
        rows = df.select("table_catalog", "table_schema", "table_name").distinct().collect()

        sizes = []
        for row in rows:
            full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
            size_bytes = None
            try:
                # Run DESCRIBE DETAIL. This works for Delta tables.
                # Non-delta tables (e.g. external parquet) might support it too,
                # or we catch exception.
                # We use spark.sql directly.
                details_df = self.spark.sql(f"DESCRIBE DETAIL {full_name}")

                # Check if sizeInBytes exists in the result
                if "sizeInBytes" in details_df.columns:
                    # Collect the first row
                    res = details_df.select("sizeInBytes").collect()
                    if res:
                        size_bytes = res[0][0]
            except Exception:
                # If table doesn't exist, is a view, or permissions fail, we just leave it None
                pass

            sizes.append((row.table_catalog, row.table_schema, row.table_name, size_bytes))

        # Create a DataFrame from the results
        # Define schema to ensure correct types (string, string, string, long)
        size_df = self.spark.createDataFrame(sizes, ["table_catalog", "table_schema", "table_name", "size_in_bytes"])

        # Join back to the original DF
        return df.join(size_df, ["table_catalog", "table_schema", "table_name"], "left")
