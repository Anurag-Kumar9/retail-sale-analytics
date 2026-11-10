"""Shared input schema and pipeline configuration."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import types as T


TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("InvoiceNo", T.StringType(), True),
        T.StructField("StockCode", T.StringType(), True),
        T.StructField("Description", T.StringType(), True),
        T.StructField("Quantity", T.IntegerType(), True),
        T.StructField("InvoiceDate", T.StringType(), True),
        T.StructField("UnitPrice", T.DoubleType(), True),
        T.StructField("CustomerID", T.DoubleType(), True),
        T.StructField("Country", T.StringType(), True),
    ]
)


@dataclass(frozen=True)
class PipelineConfig:
    input_path: Path
    output_dir: Path
    master: str = "local[*]"
    app_name: str = "RetailSalesAnalytics"
