# Databricks notebook source
# MAGIC %md
# MAGIC ### Scenario - 1
# MAGIC **-Managed Catalog**
# MAGIC **-Managed Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG man_cata

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Schema/Database**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA man_cata.man_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA man_cata.man_schema2

# COMMAND ----------

# MAGIC %md 
# MAGIC **Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.man_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.man_table3
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.man_table2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2
# MAGIC **-External Catalog**
# MAGIC **-Managed Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG ext_cata
# MAGIC MANAGED LOCATION 'abfss://mycontainer@datalake1558.dfs.core.windows.net/external_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG new_cata
# MAGIC MANAGED LOCATION 'abfss://new-container@datalake1558.dfs.core.windows.net/external_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.man_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.man_schema.man_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3
# MAGIC **-External Catalog**
# MAGIC **-External Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.ext_schema MANAGED LOCATION
# MAGIC 'abfss://mycontainer@datalake1558.dfs.core.windows.net/ext_schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.ext_schema.man_table3
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 4
# MAGIC **-External Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.ext_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  
# MAGIC LOCATION 'abfss://mycontainer@datalake1558.dfs.core.windows.net/ext_table/man_table3'

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE man_cata.man_schema.man_table3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNDROP Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE man_cata.man_schema.man_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Files using SELECT

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   man_cata.man_schema.ext_table
# MAGIC VALUES (1, 'ansh'), (2, 'lamba'), (3, 'TedX Speaker')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.ext_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://mycontainer@datalake1558.dfs.core.windows.net/ext_table/man_table3` where id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permanent Views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW man_cata.man_schema.view1
# MAGIC AS 
# MAGIC SELECT * FROM delta.`abfss://mycontainer@datalake1558.dfs.core.windows.net/ext_table/man_table3` WHERE id = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.view1

# COMMAND ----------

# MAGIC %md
# MAGIC ### TEMP VIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC **Temp View**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view
# MAGIC AS 
# MAGIC SELECT * FROM delta.`abfss://mycontainer@datalake1558.dfs.core.windows.net/ext_table/man_table3` WHERE id = 1 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC ### VOLUMES

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a directory for volume**

# COMMAND ----------

dbutils.fs.mkdirs('abfss://mycontainer@datalake1558.dfs.core.windows.net/volumes')

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME man_cata.man_schema.extvolume
# MAGIC LOCATION 'abfss://mycontainer@datalake1558.dfs.core.windows.net/volumes'

# COMMAND ----------

# MAGIC %md
# MAGIC **Copy file for volume**

# COMMAND ----------

dbutils.fs.cp('abfss://mycontainer@datalake1558.dfs.core.windows.net/source/Sales.csv', 'abfss://mycontainer@datalake1558.dfs.core.windows.net/volumes/sales')

# COMMAND ----------

# MAGIC %md
# MAGIC **Querying Volumes**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/man_cata/man_schema/extvolume/sales`

# COMMAND ----------

# dbutils.fs.ls('/Volumes/man_cata/man_schema/extvolume')
!du -sh /Volumes/man_cata/man_schema/extvolume
%du -sh /Volumes/man_cata/man_schema/extvolume

# COMMAND ----------

# MAGIC %md
# MAGIC ## spark in scala

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.catalog.listCatalogs().show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.catalog.setCurrentCatalog("man_cata")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listDatabases())

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.catalog.setCurrentDatabase("man_schema")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listTables())

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SHOW VOLUMES").show()

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC curl -L -o /Volumes/man_cata/man_schema/ui_vol/chess.zip\
# MAGIC   https://www.kaggle.com/api/v1/datasets/download/datasnaek/chess

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/man_cata/man_schema/ui_vol/
# MAGIC unzip chess.zip

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mkdirs("/Volumes/man_cata/man_schema/ui_vol/chess/")
# MAGIC dbutils.fs.mv("/Volumes/man_cata/man_schema/ui_vol/games.csv","/Volumes/man_cata/man_schema/ui_vol/chess/games.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema","true")
# MAGIC   .load("/Volumes/man_cata/man_schema/ui_vol/chess")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Samples Delta share

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC USE samples.bakehouse;
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `samples`; select * from `bakehouse`.`media_customer_reviews` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE my_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a table to the share
# MAGIC ALTER SHARE my_share ADD TABLE man_cata.man_schema.ext_table WITH HISTORY;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a recipient
# MAGIC CREATE RECIPIENT my_recipient
# MAGIC COMMENT 'Recipient for sharing data'
