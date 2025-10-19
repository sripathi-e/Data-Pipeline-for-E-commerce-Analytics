# Data-Pipeline-for-E-commerce-Analytics

---
🔹 Objective

The goal is to simulate a real-world retail data pipeline where we:

1. Ingest data from multiple sources (CSV files stored in HDFS).
2. Transform and denormalize data by joining different tables (customers, orders, order_items).
3. Store processed data back into HDFS in JSON format for downstream use.
4. Perform analytics such as:
      * Finding all customer orders on a given date.
      * Calculating monthly customer revenue.

---
🔹 Dataset (Retail Data)

We use 3 CSV files (public dataset):

1. Customers Table: 
      customer_id (PK), customer_fname, customer_lname, customer_email, …

2. Orders Table: 
      order_id (PK), order_date, order_customer_id (FK → customer_id), order_status

3. Order_Items Table: 
      order_item_id (PK), order_item_order_id (FK → order_id), order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price

👉 Relationships:

  * A customer can place many orders.

  * Each order can have many order_items.

---
🔹 Tech Stack

Hadoop (HDFS) → distributed storage for raw CSV + processed JSON
Apache Spark (PySpark) → data ingestion, transformation, and analysis
Python → project code (PySpark)
SQL-style operations using Spark DataFrames

---

🔹 Architecture / Workflow

1. Data Ingestion

   * Upload customers.csv, orders.csv, and order_items.csv into HDFS.
   * Use Spark to read CSV files with predefined schemas.

2. Data Transformation

    * Join customers + orders + order_items to create a denormalized dataset.
    * Use Spark functions like struct(), collect_list(), and explode() to nest/un-nest data.

3. Data Storage

    * Write denormalized data back into HDFS as JSON.
    * This JSON serves as a single source of truth for analytics.

4. Data Analytics

    * Query 1: Get orders placed on 2014-01-01.
    * Query 2: Compute monthly revenue per customer.

