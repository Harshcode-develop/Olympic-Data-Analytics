# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

container_name = "<container_name>"
storage_account = "<storage_acc_name>"
key = "<storage_key>"

url = "wasbs://" + container_name + "@" + storage_account + ".blob.core.windows.net/"
config = "fs.azure.account.key." + storage_account + ".blob.core.windows.net"

mount_folder = "/mnt/olympic"
dbutils.fs.mount(source = url, mount_point = mount_folder, extra_configs = {config : key})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olympic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/Raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/Raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/Raw-data/enteriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/Raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/Raw-data/teams.csv")
     

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

#top countries receiving gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

#AVG number of entries by gender
avg_entries_by_gender = entriesgender.withColumn('Avg_Female', entriesgender['Female']/entriesgender['Total']
).withColumn('Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
avg_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympic/Transformed-data/athletes")

# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/Transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/Transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/Transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/Transformed-data/teams")
