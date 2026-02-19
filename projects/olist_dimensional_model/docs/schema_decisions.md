# Schema Design Decisions

## Overview

This project implements a **star schema** dimensional model for the [Brazilian E-Commerce Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100K orders, 2016–2018). The model is built with **dbt** on **DuckDB** and follows Kimball dimensional modeling principles.

---

## Star Schema Rationale

A star schema was chosen over a snowflake or flat table approach because:

1. **Query performance** — Star joins are simple for analytical queries; BI tools can push predicates to dimension filters efficiently.
2. **Conformed dimensions** — `dim_customer`, `dim_date`, and `dim_geography` are reused across multiple fact tables, ensuring consistent analysis.
3. **Separation of concerns** — Descriptive attributes live in dimensions; measurable events live in facts. This makes the model easier to extend.

---

## Fact Tables

### `fact_order_items`

| Property         | Value                                                                           |
| ---------------- | ------------------------------------------------------------------------------- |
| **Grain**        | One row per order item                                                          |
| **Key measures** | `price`, `freight_value`, `delivery_days`, `estimated_delivery_days`, `is_late` |

**Why this grain?** An order can contain multiple items from different sellers at different prices. The order-item level preserves the most analytical granularity — you can aggregate up to order, seller, or product level as needed.

**Derived metrics:**
- `delivery_days` — actual days from purchase to delivery
- `estimated_delivery_days` — promised days from purchase to estimated delivery
- `is_late` — boolean flag when `delivered_customer_date > estimated_delivery_date`

### `fact_order_payments`

| Property         | Value                                   |
| ---------------- | --------------------------------------- |
| **Grain**        | One row per payment transaction         |
| **Key measures** | `payment_value`, `payment_installments` |

**Why a separate fact?** Orders can be paid with multiple methods (e.g., credit card + voucher) and each may have different installment counts. Mixing this into `fact_order_items` would create a many-to-many explosion.

### `fact_order_reviews`

| Property         | Value                       |
| ---------------- | --------------------------- |
| **Grain**        | One row per customer review |
| **Key measures** | `review_score`              |

**Why a separate fact?** Reviews map 1:1 to orders, not to items. Embedding them in `fact_order_items` would duplicate review data across every item in a multi-item order.

---

## Dimension Tables

### `dim_customer` — SCD Type 2

This is the most interesting dimension in the model. The Olist dataset has a subtle schema quirk:

- `customer_id` is unique **per order**, not per person.
- `customer_unique_id` is the true business key for a person.
- The same `customer_unique_id` may appear with different `zip_code_prefix`, `city`, `state` across orders.

**SCD Type 2 implementation:**

1. Join `customers` to `orders` to get the order timestamp for each address snapshot.
2. Window-function comparison detects when `zip/city/state` changes between consecutive orders for the same `customer_unique_id`.
3. Each address period gets a row with:
   - `customer_key` — surrogate integer key
   - `valid_from` / `valid_to` — date range of this address version
   - `is_current` — `true` for the most recent address

**Fact table joins** use SCD2-aware logic:
```sql
cast(order_purchase_timestamp as date) BETWEEN c.valid_from AND c.valid_to
```

This ensures each order item links to the customer's address **at the time of that order**, not their current address.

### `dim_product`

- Enriched with English category names via `product_category_name_translation`.
- Fixes the source typo: `product_name_lenght` → `product_name_length`.

### `dim_seller`

- Direct pass-through from staging with light cleaning (trim, case normalization).
- Location data (zip, city, state) is kept here rather than a junk dimension since it's inherently tied to the seller.

### `dim_date`

- Generated date spine (2016-01-01 to 2018-12-31) — no dependency on source data.
- `date_key` is in `YYYYMMDD` integer format for efficient filtering.
- Includes `is_weekend`, `day_name`, `month_name` for convenient slicing.

### `dim_geography`

- The raw geolocation data has many rows per zip code prefix.
- Coordinates are **averaged** per zip; city/state are taken from the **most frequent** occurrence.
- Natural key is `zip_code_prefix` since it's the only geographic identifier in customer/seller data.

### `dim_payment_type`

- Small junk-like dimension with surrogate keys.
- Distinct values: `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined`.

---

## Trade-offs & Alternatives Considered

| Decision                 | Alternative                 | Why We Chose This                                          |
| ------------------------ | --------------------------- | ---------------------------------------------------------- |
| Separate payment fact    | Embed in `fact_order_items` | Many-to-many between items and payments would inflate rows |
| SCD Type 2 for customers | Type 1 (overwrite)          | Need to preserve address history for delivery analysis     |
| Averaged geo coordinates | Random pick or median       | Average is more stable across the many duplicates per zip  |
| DuckDB for local dev     | PostgreSQL / SQLite         | Native CSV reading, fast OLAP queries, dbt-duckdb adapter  |
| Integer date keys        | Date type keys              | Better BI tool compatibility and partition pruning         |

---

## Data Quality Considerations

1. **Null handling** — Some orders lack `order_approved_at` or delivery timestamps. These are preserved as `NULL` rather than filtered out, so delivery metrics compute only when data exists.
2. **Duplicate geolocations** — The raw file has ~1M rows for ~20K zip codes. `dim_geography` deduplicates to one row per zip.
3. **Missing category translations** — A `LEFT JOIN` to the translation table preserves products whose category has no English name.
4. **Review deduplication** — The raw data may contain duplicate `review_id` values for the same order; dbt tests validate uniqueness after load.
