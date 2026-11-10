"""Read and prepare raw transaction rows."""

from pyspark.sql import DataFrame, functions as F

from retail_analytics.config import TRANSACTION_SCHEMA


def read_transactions(spark, input_path: str) -> DataFrame:
    """Load the source CSV using the expected UCI field types."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(TRANSACTION_SCHEMA)
        .csv(input_path)
    )


def clean_transactions(raw: DataFrame) -> DataFrame:
    """Parse dates, remove unusable line items, and derive sales fields."""
    return (
        # The bundled downloader emits ISO timestamps through pandas; accepting
        # the original UCI timestamp form also supports independently made CSVs.
        raw.withColumn(
            "InvoiceDate",
            F.coalesce(
                F.try_to_timestamp("InvoiceDate", F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp("InvoiceDate", F.lit("M/d/yyyy H:mm")),
            ),
        )
        .withColumn("CustomerID", F.col("CustomerID").cast("long"))
        .filter(
            F.col("InvoiceNo").isNotNull()
            & F.col("StockCode").isNotNull()
            & F.col("InvoiceDate").isNotNull()
            & F.col("Quantity").isNotNull()
            & F.col("UnitPrice").isNotNull()
            & (F.col("Quantity") != 0)
            & (F.col("UnitPrice") > 0)
        )
        .withColumn("line_revenue", F.col("Quantity") * F.col("UnitPrice"))
        .withColumn("invoice_date", F.to_date("InvoiceDate"))
        .withColumn("month", F.date_format("InvoiceDate", "yyyy-MM"))
        .withColumn("gross_sales", F.when(F.col("line_revenue") > 0, F.col("line_revenue")).otherwise(0.0))
        .withColumn("returns", F.when(F.col("line_revenue") < 0, -F.col("line_revenue")).otherwise(0.0))
    )
