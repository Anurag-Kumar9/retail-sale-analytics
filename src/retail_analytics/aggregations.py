"""Spark transformations for business-facing sales summaries."""

from pyspark.sql import DataFrame, functions as F


def _sales_metrics():
    return [
        F.round(F.sum("gross_sales"), 2).alias("gross_sales"),
        F.round(F.sum("returns"), 2).alias("returns"),
        F.round(F.sum("line_revenue"), 2).alias("net_sales"),
        F.countDistinct("InvoiceNo").alias("orders"),
    ]


def daily_sales(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("invoice_date").agg(*_sales_metrics()).orderBy("invoice_date")


def monthly_sales(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("month").agg(*_sales_metrics()).orderBy("month")


def product_sales(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("StockCode", "Description")
        .agg(
            F.sum("Quantity").alias("net_units"),
            F.round(F.sum("line_revenue"), 2).alias("net_sales"),
            F.countDistinct("InvoiceNo").alias("orders"),
        )
        .orderBy(F.desc("net_sales"))
    )


def country_sales(transactions: DataFrame) -> DataFrame:
    return (
        transactions.filter(F.col("Country").isNotNull())
        .groupBy("Country")
        .agg(
            F.sum("Quantity").alias("net_units"),
            F.round(F.sum("line_revenue"), 2).alias("net_sales"),
            F.countDistinct("InvoiceNo").alias("orders"),
        )
        .orderBy(F.desc("net_sales"))
    )
