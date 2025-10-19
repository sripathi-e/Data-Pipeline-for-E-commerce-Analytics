from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import struct, collect_list, explode, year, month, sum as _sum

# ----------------------------
# Step 1: Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("Retail Data Processing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# ----------------------------
# Step 2: Define Schemas
# ----------------------------
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("customer_lname", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_password", StringType(), True),
    StructField("customer_street", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("customer_zipcode", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_item_order_id", IntegerType(), True),
    StructField("order_item_product_id", IntegerType(), True),
    StructField("order_item_quantity", IntegerType(), True),
    StructField("order_item_subtotal", DoubleType(), True),
    StructField("order_item_product_price", DoubleType(), True)
])

# ----------------------------
# Step 3: Load Data from HDFS
# ----------------------------
customers_df = spark.read.csv("hdfs://localhost:9000/user/retail/customers/customers.csv", schema=customer_schema)
orders_df = spark.read.csv("hdfs://localhost:9000/user/retail/orders/orders.csv", schema=orders_schema)
order_items_df = spark.read.csv("hdfs://localhost:9000/user/retail/order_items/order_items.csv", schema=order_items_schema)

# ----------------------------
# Step 4: Join Customers + Orders + Order_Items
# ----------------------------
customers_orders_df = customers_df.join(orders_df, customers_df['customer_id'] == orders_df['order_customer_id'])
full_df = customers_orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'])

# ----------------------------
# Step 5: Denormalize Data
# ----------------------------
customer_order_struct = full_df.select(
    'customer_id', 'customer_fname',
    struct('order_id', 'order_date', 'order_status',
           'order_item_id', 'order_item_product_id',
           'order_item_quantity', 'order_item_subtotal').alias('order_details')
)

final_df = customer_order_struct.groupBy('customer_id','customer_fname') \
    .agg(collect_list('order_details').alias('order_details')) \
    .orderBy('customer_id')

# Save denormalized data as JSON in HDFS
final_df.coalesce(1).write.mode("overwrite").json("hdfs://localhost:9000/user/retail/denorm")

# ----------------------------
# Step 6: Analytics
# ----------------------------

## Q1: Orders placed on 2014-01-01
exploded_df = final_df.select("customer_id","customer_fname", explode("order_details").alias("order_details"))
exploded_df.filter("order_details.order_date LIKE '2014-01-01%'") \
    .select("customer_id","customer_fname","order_details.order_id","order_details.order_date","order_details.order_status") \
    .show(10, False)

## Q2: Monthly Customer Revenue
monthly_revenue = full_df.withColumn("order_year", year("order_date").cast("int")) \
    .withColumn("order_month", month("order_date").cast("int")) \
    .groupBy("customer_id","order_year","order_month") \
    .agg(_sum("order_item_subtotal").alias("monthly_revenue")) \
    .orderBy("customer_id","order_year","order_month")

monthly_revenue.show(20, False)

# ----------------------------
# End Spark
# ----------------------------
spark.stop()
