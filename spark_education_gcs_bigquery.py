#most of this code comes from the data engineering zoomcamp course
# see below for the transformations
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# i will put here credentials and necessary jars informations
credentials_location = '/home/z01_de/creds/data-eng-cr-32a0fe16e290.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/z01_de/lib/gcs-connector-hadoop3-2.2.5.jar,/home/z01_de/lib/spark-bigquery.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)\
    .set('temporaryGcsBucket', 'data-eng-cr-bucket')

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# the data is loaded from gcs - all of the parquet files at once
df = spark.read.parquet('gs://data-eng-cr-bucket/data/parquet/*')

# we filtered previously the data in this column, so we do not need it anymore
df=df.drop("SUBGROUP_NAME")

# for convenience this variable was string until now. Now, it needs to become an int
df=df.withColumn("PER_PROF",df.PER_PROF.cast('int'))

# let us make a wide table, where the three subjects ELA, math and science appear for every row of interest (entity and year_semester)
pivotDF = df.groupBy(['ENTITY_NAME','YEAR_SEMESTER']).pivot("ASSESSMENT_NAME").sum("PER_PROF")

print(pivotDF.head(5))

# a new variable will hold the averageover the three subjects
pivotDF=pivotDF.withColumn("AVERAGE_PER_PROF",F.round((pivotDF.ELA8+pivotDF.MATH8+pivotDF.Science8)/3)) 

# for convenience drop the other values
pivotDF=pivotDF.drop("ELA8","MATH8","Science8")

# pivot also this dataframe so to have the year_semester values in the columns
df = df.groupBy(['ENTITY_NAME']).pivot('YEAR_SEMESTER').sum("PER_PROF")

#calculate the change of the value between the newest year and the oldest year with valid data
df=df.withColumn("TIME_CHANGE",df['2022S1']-df['2018S1'])

#only select the two valid columns
df=df.select("ENTITY_NAME","TIME_CHANGE")

#now write the data to bigquery
pivotDF.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .save("education_database.timeseries")

df.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .save("education_database.timechange")