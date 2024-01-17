# Databricks notebook source
# MAGIC %fs ls  dbfs:/mnt/adlsshelldatabricks/raw/

# COMMAND ----------

# MAGIC %sql
# MAGIC use sudeep;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sudeep.people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) location 'dbfs:/mnt/adlsshelldatabricks/raw/delta/sudeep/people10m'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sudeep.people10m 
