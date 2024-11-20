# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<clientid>",
          "fs.azure.account.oauth2.client.secret":"<secret key>",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenantid>/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://tokyo-olympic-data@tokyoolympicdatasstore.dfs.core.windows.net",
  mount_point = "/mnt/tokyoolym",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.ls("/mnt/tokyoolym")

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolym/rawdata/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolym/rawdata/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolym/rawdata/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolym/rawdata/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolym/rawdata/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
   .withColumn("Male", col("Male").cast(IntegerType()))\
     .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

##find countries with highest number of gold medals
highest_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# COMMAND ----------

##find the average number of entries by gender of each discipline
avg_entries_by_gender = entriesgender.withColumn("avg_female",entriesgender['Female']/entriesgender['Total']).withColumn("avg_male",entriesgender['Male']/entriesgender['Total'])
avg_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolym/transformeddata/athletes.csv")
coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolym/transformeddata/coaches.csv")
entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolym/transformeddata/entriesgender.csv")
medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolym/transformeddata/medals.csv")
teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolym/transformeddata/teams.csv")