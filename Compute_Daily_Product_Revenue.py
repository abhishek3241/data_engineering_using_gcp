from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round
import os

bucket_name = os.environ.get('BUCKET_NAME')

spark = SparkSession.builder.master('yarn').getOrCreate()

#Creating orders and order_items DataFrame
orders = spark.read.parquet(f'gs://{bucket_name}/retail_db_parquet/orders/')
order_items = spark.read.parquet(f'gs://{bucket_name}/retail_db_parquet/order_items/')

#Computing Daily Product revenue for orders in COMPLETE or CLOSED status
daily_product_revenue = orders. \
                        filter("order_status in ('COMPLETE' , 'CLOSED')"). \
                        join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
                        groupBy('order_date', 'order_item_product_id'). \
                        agg(round(sum('order_item_subtotal'),2).alias('daily_product_revenue')). \
                        orderBy('order_date',col('daily_product_revenue').desc())
                        

#Writing daily_product_revenue Data to GCS
daily_product_revenue. \
            write. \
            parquet(f'gs://{bucket_name}/retail_db_parquet/daily_product_revenue', mode = 'overwrite')