# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use catalog my_catalog;
# MAGIC use schema test

# COMMAND ----------

data = [(1,"Adam","USA"),(2,"Bob","Canada"),(3,"Charlie","UK"),(4,"David","Australia")]
schema  = ["id","name","country"]
df = spark.createDataFrame(data,schema)
df.display()


# COMMAND ----------

df.write.mode("overwrite").save('/Volumes/my_catalog/test/test_volume/test_data1')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/my_catalog/test/test_volume/test_data1

# COMMAND ----------

# MAGIC %fs ls /Volumes/my_catalog/test/test_volume/test_data1/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/my_catalog/test/test_volume/test_data1/_delta_log/00000000000000000000.json`

# COMMAND ----------

data = [(5,"Gilbert","Ireland"),(6,"Hannah","France"),(7,"Ivan","Germany")]
schema  = ["id","name","country"]
df_new = spark.createDataFrame(data,schema)
df_new.display()


# COMMAND ----------

df_new.write.mode("append").save('/Volumes/my_catalog/test/test_volume/test_data1')

# COMMAND ----------

# MAGIC %fs ls /Volumes/my_catalog/test/test_volume/test_data1/

# COMMAND ----------

# MAGIC %fs ls /Volumes/my_catalog/test/test_volume/test_data1/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/my_catalog/test/test_volume/test_data1/_delta_log/00000000000000000001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/my_catalog/test/test_volume/test_data1`