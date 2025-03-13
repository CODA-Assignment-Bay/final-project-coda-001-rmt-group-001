from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime

#Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

corridor_dimension = spark.read.csv('/opt/airflow/data/corridor_dimension', header=True, inferSchema=True)
date_dimension = spark.read.csv('/opt/airflow/data/date_dimension', header=True, inferSchema=True)
halte_dimension = spark.read.csv('/opt/airflow/data/halte_dimension', header=True, inferSchema=True)
transaction_fact = spark.read.csv('/opt/airflow/data/transaction_fact', header=True, inferSchema=True)
user_dimension = spark.read.csv('/opt/airflow/data/user_dimension', header=True, inferSchema=True)

postgres_url = "jdbc:postgresql://ep-spring-frost-a8wjwj5n-pooler.eastus2.azure.neon.tech:5432/neondb?user=neondb_owner&password=npg_lwmY8XBgr9dh"

postgres_properties = {
    "user": "neondb_owner",
    "password": "npg_lwmY8XBgr9dh",
    "driver": "org.postgresql.Driver"
}

corridor_dimension.write.jdbc(url=postgres_url, table="corridor_dimension", mode="overwrite", properties=postgres_properties)
date_dimension.write.jdbc(url=postgres_url, table="date_dimension", mode="overwrite", properties=postgres_properties)
halte_dimension.write.jdbc(url=postgres_url, table="halte_dimension", mode="overwrite", properties=postgres_properties)
transaction_fact.write.jdbc(url=postgres_url, table="transaction_fact", mode="overwrite", properties=postgres_properties)
user_dimension.write.jdbc(url=postgres_url, table="user_dimension", mode="overwrite", properties=postgres_properties)