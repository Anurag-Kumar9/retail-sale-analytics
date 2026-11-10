"""Coordinate the Spark session and analytics stages."""

from retail_analytics.aggregations import country_sales, daily_sales, monthly_sales, product_sales
from retail_analytics.config import PipelineConfig
from retail_analytics.ingestion import clean_transactions, read_transactions
from retail_analytics.segmentation import customer_rfm


def run_pipeline(config: PipelineConfig) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(config.app_name).master(config.master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        transactions = clean_transactions(read_transactions(spark, str(config.input_path)))
        outputs = {
            "daily_sales": daily_sales(transactions),
            "monthly_sales": monthly_sales(transactions),
            "product_sales": product_sales(transactions),
            "country_sales": country_sales(transactions),
            "customer_rfm": customer_rfm(transactions),
        }
        for name, dataframe in outputs.items():
            dataframe.write.mode("overwrite").option("header", True).csv(str(config.output_dir / name))
            print(f"Wrote {name}")
        print(f"Analytics complete. Output tables are in {config.output_dir}")
    finally:
        spark.stop()
