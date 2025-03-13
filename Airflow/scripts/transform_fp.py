import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
from pyspark.sql.functions import col, count, first, sequence, to_timestamp, explode, date_format, when, quarter, year, month, expr, dayofmonth, min, max
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_timestamp
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .getOrCreate()

csv_file_path = "/opt/airflow/data/extract_fp.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
###################################################################################################
# mengisi nilai null dengan unknown pada kolom corridorID
df = df.fillna({'corridorID': 'unknown'})
# mengisi nilai null dengan unknown pada kolom corridorName
df = df.fillna({'corridorName': 'unknown'})
# mengisi nilai null dengan unknown pada kolom tapInStops
df = df.fillna({'tapInStops': 'unknown'})
# mengisi nilai null dengan unknown pada kolom tapOutStops
df = df.fillna({'tapOutStops': 'unknown'})
# mengisi nilai null dengan unknown pada kolom tapOutStopsName
df = df.fillna({'tapOutStopsName': 'unknown'})

#ubah tipe data tapouttime ke timestamp
df = df.withColumn("tapOutTime", to_timestamp("tapOutTime"))
#ubah tipe data tapintime ke timestamp
df = df.withColumn("tapInTime", to_timestamp("tapInTime"))

#######################################################################################
# create corridor_dimension
corridor_dimension = df.select("corridorID", "corridorName").dropDuplicates()
#######################################################################################
# create user_dimension
user_dimension = df.select("payCardID", "payCardBank", "payCardName", "payCardSex", "payCardBirthDate") \
                   .dropDuplicates()
#######################################################################################
# create date_dimension
df.select(
    min("tapInTime").alias("tanggal_awal"),
    max("tapInTime").alias("tanggal_akhir")
)
df.select(
    min("tapOutTime").alias("tanggal_awal"),
    max("tapOutTime").alias("tanggal_akhir")
)
start = "2023-01-01 00:00:00"
end = "2025-12-31 00:00:00"

#Buat df_date
date_dimension = spark.sql(f"SELECT sequence(to_timestamp('{start}'), to_timestamp('{end}'), interval 1 day) as dates")

#Pecah kolom date
date_dimension = date_dimension.withColumn("date", explode(col("dates"))).drop("dates")

#Tambah kolom berdasarkan kolom date
date_dimension = date_dimension.withColumn("month", month(col("date"))) \
       .withColumn("quarter", quarter(col("date"))) \
       .withColumn("year", year(col("date"))) \
       .withColumn("half", expr("case when month(date) <= 6 then 1 else 2 end"))

#######################################################################################
gdf_jakbar = gpd.read_file("/opt/airflow/data/JAKARTABARAT/ADMINISTRASIKECAMATAN_AR_25K.shp")
gdf_jakpus = gpd.read_file("/opt/airflow/data/JAKARTAPUSAT/ADMINISTRASIKECAMATAN_AR_25K.shp")
gdf_jaktim = gpd.read_file("/opt/airflow/data/JAKARTATIMUR/ADMINISTRASIKECAMATAN_AR_25K.shp")
gdf_jakut = gpd.read_file("/opt/airflow/data/JAKARTAUTARA/ADMINISTRASIKECAMATAN_AR_25K.shp")
gdf_jaksel = gpd.read_file("/opt/airflow/data/JAKARTASELATAN/ADMINISTRASIKECAMATAN_AR_25K.shp")

def get_kota(lon, lat):
    point = Point(lon, lat)
    if gdf_jakbar.contains(point).any():
        return 'Jakarta Barat'
    elif gdf_jakpus.contains(point).any():
        return 'Jakarta Pusat'
    elif gdf_jaktim.contains(point).any():
        return 'Jakarta Timur'
    elif gdf_jakut.contains(point).any():
        return 'Jakarta Utara'
    elif gdf_jaksel.contains(point).any():
        return 'Jakarta Selatan'

get_kota_udf = udf(get_kota, StringType())
df = df.withColumn("kota_tapin", get_kota_udf(df["tapInStopsLon"], df["tapInStopsLat"]))
df = df.withColumn("kota_tapout", get_kota_udf(df["tapOutStopsLon"], df["tapOutStopsLat"]))

df_tapin = df.select(
    col("tapInStops").alias("halte_ID"),
    col("tapInStopsName").alias("halte_Name"),
    col("kota_tapin").alias("halte_City"),
    col("tapInStopsLat").alias("halteLat"),
    col("tapInStopsLon").alias("halteLon")
)

df_tapout = df.select(
    col("tapOutStops").alias("halte_ID"),
    col("tapOutStopsName").alias("halte_Name"),
    col("kota_tapout").alias("halte_City"),
    col("tapOutStopsLat").alias("halteLat"),
    col("tapOutStopsLon").alias("halteLon")
)

halte_dimension = df_tapin.union(df_tapout)
halte_dimension = halte_dimension.withColumn("halte_City", when(col("halte_City").isNull(), "Luar Jakarta").otherwise(col("halte_City")))
halte_dimension = halte_dimension.dropDuplicates()
halte_dimension = halte_dimension.filter(col("halte_ID") != "unknown")
#######################################################################################
### transaction_fact
df = df.withColumn("duration", col("tapOutTime") - col("tapInTime"))
df = df.withColumn("seqAmount", col("stopEndSeq") - col("stopStartSeq"))
df = df.withColumn('transaction_created_at', to_date(df['tapInTime']))

transaction_fact = df.select(
    'transID', 'payCardID', 'tapInStops', 'tapOutStops', 'corridorID', 
    'transaction_created_at', 'tapInTime', 'tapOutTime', 'duration',
    'stopStartSeq', 'stopEndSeq', 'seqAmount', 'payAmount'
)

transaction_fact = transaction_fact.withColumnRenamed('tapInStops', 'halteID_tapIn') \
                                     .withColumnRenamed('tapOutStops', 'halteID_tapOut')

transaction_fact = transaction_fact.withColumn("duration", F.expr("EXTRACT(MINUTE FROM duration)"))

transaction_fact.write.csv('/opt/airflow/data/transaction_fact', header=True, mode="overwrite")
halte_dimension.write.csv('/opt/airflow/data/halte_dimension', header=True, mode="overwrite")
date_dimension.write.csv('/opt/airflow/data/date_dimension', header=True, mode="overwrite")
user_dimension.write.csv('/opt/airflow/data/user_dimension', header=True, mode="overwrite")
corridor_dimension.write.csv('/opt/airflow/data/corridor_dimension', header=True, mode="overwrite")