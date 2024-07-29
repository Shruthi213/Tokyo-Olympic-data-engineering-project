# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com//oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-project-container@olympictokyoproject123.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-directory/Athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-directory/Coaches.csv")
entriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-directory/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-directory/Medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-directory/Teams.csv")

# COMMAND ----------

athletes.show()


# COMMAND ----------

athletes.printSchema()


# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesGender.show()

# COMMAND ----------

entriesGender.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

entriesGender.printSchema()

# COMMAND ----------

entriesGender = entriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/Athletes")

# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/Coaches")
entriesGender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/EntriesGender")
medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/Medals")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/Teams")
