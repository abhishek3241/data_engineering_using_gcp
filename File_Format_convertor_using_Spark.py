from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.master('yarn').getOrCreate()

bucket_name = os.environ.get('BUCKET_NAME')
tgt_name = os.environ.get('TGT_NAME')

orders = spark.read.csv(f'gs://{bucket_name}/retail_db/orders/part-00000' , schema ='order_id int, order_date date, order_customer_id int, order_status string')

order_items = spark. \
    read. \
        csv(f'gs://{bucket_name}/retail_db/order_items/part-00000' , 
            schema ='order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, order_item_subtotal float, order_item_product_price float'
            )

orders. \
    write. \
    parquet(
        path = f'gs://abhiretail/{tgt_name}/orders',
        mode = 'overwrite'
    )

order_items. \
    write. \
    parquet(
        path = f'gs://abhiretail/{tgt_name}/order_items',
        mode = 'overwrite'
    )

