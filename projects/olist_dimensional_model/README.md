# Olist E-Commerce Dimensional Model

Dimensional data model for the [Brazilian E-Commerce Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) built with **dbt** + **DuckDB**.

## Quick Start

```bash
# 1. Install dependencies
cd /path/to/olist_dimensional_model
uv sync

# 2. Download the Olist dataset from Kaggle and place CSVs in data/raw/
#    Expected files:
#    - olist_customers_dataset.csv
#    - olist_orders_dataset.csv
#    - olist_order_items_dataset.csv
#    - olist_order_payments_dataset.csv
#    - olist_order_reviews_dataset.csv
#    - olist_products_dataset.csv
#    - olist_sellers_dataset.csv
#    - olist_geolocation_dataset.csv
#    - product_category_name_translation.csv

# 3. Run dbt models
cd transform
uv run dbt run --profiles-dir .

# 4. Run dbt tests
uv run dbt test --profiles-dir .
```

## Schema

See [docs/schema_decisions.md](docs/schema_decisions.md) for detailed design rationale.

### Dimensions
| Table              | Description                                       |
| ------------------ | ------------------------------------------------- |
| `dim_customer`     | SCD Type 2 — tracks address changes across orders |
| `dim_product`      | Product catalog with English category names       |
| `dim_seller`       | Seller locations                                  |
| `dim_date`         | Date spine (2016–2018)                            |
| `dim_geography`    | Zip code → city/state/coordinates                 |
| `dim_payment_type` | Payment method lookup                             |

### Facts
| Table                 | Grain                           |
| --------------------- | ------------------------------- |
| `fact_order_items`    | One row per order item          |
| `fact_order_payments` | One row per payment transaction |
| `fact_order_reviews`  | One row per customer review     |
