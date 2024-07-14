import os
from pyspark.sql import SparkSession

spark = SparkSession. \
        builder. \
        getOrCreat()

#Define environment variables to pass values dynamically

data_uri = os.environ.get('DATA_URI')
project_id = os.environ.get('PROJECT_ID')
dataset_name = os.environ.get('DATASET_NAME')
gcs_temp_bucket = os.environ.get('GCS_TEMP_BUCKET')

#create DF
daily_product_revenue = spark. \
                            read. \
                            parquet(data_uri)

#set below property to stage data in temporary location in gcs  before writting it to target BQ table.

spark.conf.get('temporaryGcsBucket' , gcs_temp_bucket)

#write data to GCP-BQ using Spark:

daily_product_revenue. \
                write. \
                mode('overwrite'). \
                format('bigquery'). \
                option('table', f'{project_id}:{dataset_name}.daily_product_revenue'). \
                save()

#set environment variables inside DP :

export DATA_URI = 'gs://'
export PROJECT_ID = ''
export DATASET_NAME = ''
export GCS_TEMP_BUCKET = ''

#validate the same using echo $DATA_URI etc

#spark submit command to submit above application in client mode:

spark-submit --jars gs://jarpath file_name.py

#spark submit command to submit application in cluster mode : 

spark-submit \
        --master yarn \
        --deply-mode cluster \
        --name "Daily product Revenue Loader" \
        --jars gs://jarpath \
        --conf "spark.yarn.appMasterEnv.DATA_URI=gs://" \
        --conf "spark.yarn.appMasterEnv.PROJECT_ID = \
        --conf "spark.yarn.appMasterEnv.DATASET_NAME = \
        --conf "spark.yarn.appMasterEnv.GCS_TEMP_BUCKET= \
        app_name.py


# To deploy your application from GCS we need to copy the the app_name.py to GCS and run the below command by just replaceing the file path with gcs path:

spark-submit \
        --master yarn \
        --deply-mode cluster \
        --name "Daily product Revenue Loader" \
        --jars gs://jarpath \
        --conf "spark.yarn.appMasterEnv.DATA_URI=gs://" \
        --conf "spark.yarn.appMasterEnv.PROJECT_ID = \
        --conf "spark.yarn.appMasterEnv.DATASET_NAME = \
        --conf "spark.yarn.appMasterEnv.GCS_TEMP_BUCKET= \
        gs://app_name.py


#Running spark application using spark from local environment using gcloud dataproc command:

gcloud dataproc jobs submit \
        pyspark --cluster = cluster_name \
        --jars=gs://jarpath \
        --properties=spark.app.name="Bigquery Loader -Daily Product Revenue",spark.submit.deployMode=cluster,spark.yarn.appMasterEnv.DATE_URI=gs://, define other environment variables \
        gs://app_name.py