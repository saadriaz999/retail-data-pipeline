# Retail Sales Analytics

This project simulates a simple retail data pipeline similar to systems used in companies like Amazon and Walmart.  
It generates synthetic retail transaction data and exposes daily sales through a Flask API.

The goal is to simulate a real-world architecture where transactional data is generated, stored, and accessed via APIs for downstream analytics.

---

## Project Overview

This project includes:
- Synthetic retail dataset generation  
- Star schema design  
- Fact and dimension tables  
- A Flask API to fetch today's sales  
- Support for daily ingestion pipelines  

This can be extended for:
- AWS data pipelines  
- S3 storage  
- Athena queries  
- BI dashboards such as Looker Studio  

---

## Schema Design

The project uses a **star schema**, which is standard in modern data warehouses.

---

### Fact Table

#### fact_sales

Stores transactional retail data.

| Column | Description |
|------|------|
| sale_id | Unique row identifier |
| order_id | Transaction ID |
| date_id | Foreign key to date dimension |
| product_id | Foreign key to product |
| customer_id | Foreign key to customer |
| store_id | Foreign key to store |
| quantity | Units sold |
| unit_price | Price at time of sale |
| discount | Discount applied |
| total_amount | Final revenue |
| payment_method | Payment type |
| device_type | Mobile or desktop |

---

### Date Dimension

#### dim_date

| Column | Description |
|------|------|
| date_id | YYYYMMDD key |
| full_date | Actual date |
| day | Day of month |
| month | Month |
| year | Year |
| quarter | Quarter of year |
| day_of_week | Numeric weekday |
| is_weekend | Boolean flag |
---

### Customer Dimension

#### dim_customer

| Column | Description |
|------|------|
| customer_id | Unique ID |
| country | IP-based inferred country |
| language | Browser language |

---

### Product Dimension

#### dim_product

| Column | Description |
|------|------|
| product_id | Unique ID |
| product_name | Furniture item |
| category | Furniture |
| subcategory | Chair, table, etc. |
| brand | Brand |
| unit_cost | Cost of product |

---

### Store Dimension

#### dim_store

| Column | Description |
|------|------|
| store_id | Unique ID |
| store_name | Store |
| city | Location |
| state | Region |
| country | Country |

---
