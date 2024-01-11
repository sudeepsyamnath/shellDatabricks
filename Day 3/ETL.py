# Databricks notebook source
dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True)
df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"*").display()

# COMMAND ----------

df.withColumnRenamed("circuitid","circuit_id").display()

# COMMAND ----------

df.withColumn("newcolumn",concat("location",lit(" "),"country")).display()

# COMMAND ----------

df.withColumn("injestionDate",current_timestamp()).display()


# COMMAND ----------

df.withColumn("injestionDate",current_timestamp()).drop("url").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date() as curDate;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC

# COMMAND ----------

df.write.parquet("dbfs:/mnt/adlsshelldatabricks/raw/processed/sudeep/circuit")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema sudeep

# COMMAND ----------

df.write.saveAsTable("sudeep.circuit")
