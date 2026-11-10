"""Customer-level RFM features and interpretable rule-based segments."""

from pyspark.sql import DataFrame, functions as F


def customer_rfm(transactions: DataFrame) -> DataFrame:
    customer_rows = transactions.filter(F.col("CustomerID").isNotNull())
    snapshot = customer_rows.agg(F.date_add(F.max("invoice_date"), 1).alias("day")).first()["day"]
    rfm = customer_rows.groupBy("CustomerID").agg(
        F.datediff(F.lit(snapshot), F.max("invoice_date")).alias("recency_days"),
        F.countDistinct("InvoiceNo").alias("frequency"),
        F.round(F.sum("line_revenue"), 2).alias("monetary"),
    )

    positive = rfm.filter(F.col("monetary") > 0)
    r_quantile = positive.approxQuantile("recency_days", [0.5], 0.01)
    f_quantile = positive.approxQuantile("frequency", [0.5], 0.01)
    m_quantile = positive.approxQuantile("monetary", [0.5], 0.01)
    r_median = r_quantile[0] if r_quantile else 0
    f_median = f_quantile[0] if f_quantile else 0
    m_median = m_quantile[0] if m_quantile else 0

    return rfm.withColumn(
        "segment",
        F.when(F.col("monetary") <= 0, "At risk / net return")
        .when(
            (F.col("recency_days") <= r_median)
            & (F.col("frequency") >= f_median)
            & (F.col("monetary") >= m_median),
            "Champions",
        )
        .when((F.col("recency_days") <= r_median) & (F.col("frequency") >= f_median), "Loyal")
        .when((F.col("recency_days") <= r_median) | (F.col("frequency") >= f_median), "Potential loyalist")
        .otherwise("Needs attention"),
    ).orderBy("CustomerID")
