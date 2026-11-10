# Retail Sales Analytics with PySpark

## Project report

### Abstract

This project analyzes online retail transactions with Python and Apache Spark. It produces reusable sales summaries and customer RFM segments from invoice line items. Spark is used for ingestion, cleaning, aggregation, and writing the output tables, so the same workflow can run locally or scale to a Spark cluster.

### Problem statement

Retail teams need to understand how sales change over time, which products and markets contribute revenue, and how customer purchasing patterns differ. This project turns raw transaction rows into those decision-support views.

### Dataset

The project uses the [UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail). UCI describes it as transactions from a UK-based non-store online retailer between 1 December 2010 and 9 December 2011. The source workbook contains invoice number, product code and description, quantity, invoice date, unit price, customer ID, and country. The dataset has missing values and includes cancellation/return rows; this pipeline keeps negative quantities so returns affect net sales.

The workbook is not committed to this repository. Run the downloader to retrieve it from UCI and create a CSV copy under `data/raw/`.

### Objectives and questions

- What are daily and monthly gross sales, returns, net sales, and order counts?
- Which products and countries contribute the most net sales?
- How recently and frequently do customers purchase, and what is their net spend?
- How can customers be grouped into simple, explainable RFM segments?

### Technology

- Python 3.10+
- Apache Spark through PySpark (local mode by default)
- Pandas and openpyxl for converting the UCI Excel workbook to CSV

Kafka is not included: the selected source is a historical batch dataset, so Spark batch processing is the more appropriate syllabus technology. A Kafka extension would need a live or simulated event source.

### Pipeline and data treatment

1. `scripts/download_data.py` downloads the source workbook and converts it to CSV.
2. `scripts/run_analytics.py` starts the modular PySpark application. `ingestion.py` reads and cleans rows; `aggregations.py` and `segmentation.py` build the analysis tables.
3. Rows without invoice date, invoice number, product code, quantity, or unit price are removed. Zero quantities and nonpositive unit prices are also removed. Missing customer IDs remain available for product and geography summaries but are omitted from customer-level RFM.
4. `line_revenue = Quantity × UnitPrice`. Negative quantities (including cancellation/return lines) are retained and reduce net revenue. Gross sales sum positive line revenue; returns sum the magnitude of negative revenue.
5. Spark writes daily sales, monthly sales, product sales, country sales, and customer RFM tables as CSV directories under `data/processed/`.

### RFM method

For each identified customer, recency is measured in days from the last invoice date in the dataset plus one day; frequency is the distinct invoice count; monetary value is total net line revenue. Customers with nonpositive monetary value are labeled `At risk / net return`. Remaining customers are split at the median for each R, F, and M measure. Recency is reversed so lower recency is better. The resulting explainable labels are `Champions`, `Loyal`, `Potential loyalist`, or `Needs attention`. These are descriptive rules, not a trained predictive model.

### Outputs

| Output | Grain | Main fields |
|---|---|---|
| `daily_sales` | Invoice date | gross sales, returns, net sales, distinct orders |
| `monthly_sales` | Calendar month | gross sales, returns, net sales, distinct orders |
| `product_sales` | Product code and description | units sold, net sales, distinct orders |
| `country_sales` | Country | units sold, net sales, distinct orders |
| `customer_rfm` | Customer ID | recency, frequency, monetary, segment |

Each output is a Spark CSV directory and may contain multiple part files. To create a single file for a small result, adjust the writer in the script to use `coalesce(1)`.

### Run the project

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/download_data.py
python scripts/run_analytics.py --input data/raw/online_retail.csv --output data/processed
```

Run the commands from the activated environment. If you already created an environment named `venv`, activate it with `source venv/bin/activate` (Windows: `venv\\Scripts\\activate`) before installing requirements and running the scripts. Running with a different system Python may fail with `ModuleNotFoundError: pyspark`.

The analytics code is also runnable as a package after adding `src` to `PYTHONPATH`:

```bash
PYTHONPATH=src python -m retail_analytics --master 'local[*]'
```

Java is required by Spark. Set `JAVA_HOME` if Java is installed but not detected. To use an existing Spark cluster, pass a master URL such as `--master spark://host:7077`; otherwise the script runs in local mode.

### Interpretation and limitations

The results describe one retailer over a historical period and should not be generalized to all retail businesses. Customer IDs are absent on some records, and the dataset does not include costs, inventory, marketing exposure, or customer demographics. Revenue is therefore sales value, not profit. Median-based RFM labels are useful for exploration but should be checked against business context before operational use. No numeric findings are claimed in this report because the dataset is downloaded at runtime and has not been analyzed in this repository.

### Future work

- Add charts or a dashboard over the generated tables.
- Evaluate customer segments against later repeat purchases.
- Add product co-purchase analysis.
- Add Kafka ingestion for a live transaction stream and micro-batch processing.

### References

- Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
- Dataset page and files: https://archive.ics.uci.edu/dataset/352/online+retail

## Repository structure

```text
retail_sale/
├── data/                 # Downloaded and generated data (gitignored)
├── scripts/
│   ├── download_data.py
│   └── run_analytics.py
├── src/retail_analytics/
│   ├── aggregations.py   # Daily, monthly, product, and country summaries
│   ├── cli.py            # Arguments and input validation
│   ├── config.py         # Schema and pipeline configuration
│   ├── ingestion.py      # Reading and cleaning
│   ├── pipeline.py       # Orchestration and output writing
│   └── segmentation.py  # Customer RFM analysis
├── .gitignore
├── requirements.txt
└── README.md
```
