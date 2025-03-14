-- Databricks notebook source
use database man_schema;
show volumes;
DESCRIBE VOLUME extvolume;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import kagglehub
-- MAGIC
-- MAGIC # Download latest version
-- MAGIC path = kagglehub.dataset_download("asaniczka/top-spotify-songs-in-73-countries-daily-updated")
-- MAGIC
-- MAGIC print("Path to dataset files:", path)
-- MAGIC # /root/.cache/kagglehub/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews/versions/1
-- MAGIC dbutils.fs.ls("/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELTA LAKE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_imdb = spark.read.format("csv")\
-- MAGIC             .option("header", "true")\
-- MAGIC             .option("inferSchema", "true")\
-- MAGIC             .load("/Volumes/man_cata/man_schema/extvolume/spotify//")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_imdb.write.format('delta')\
-- MAGIC         .mode('overwrite')\
-- MAGIC         .save('abfss://destination@datalake1558.dfs.core.windows.net/spotify')

-- COMMAND ----------

select * from DELTA.`abfss://destination@datalake1558.dfs.core.windows.net/spotify` order by country, daily_rank;

-- COMMAND ----------

select distinct name, artists from DELTA.`abfss://destination@datalake1558.dfs.core.windows.net/spotify` where artists like 'John%'

-- COMMAND ----------

UPDATE DELTA.`abfss://destination@datalake1558.dfs.core.windows.net/spotify` set country='GLOBAL' where country is null ;

-- COMMAND ----------

DELETE from DELTA.`abfss://destination@datalake1558.dfs.core.windows.net/spotify` where country rlike '[^a-zA-Z]';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Load the data from the delta path
-- MAGIC df_delta = spark.read.format('delta').load('abfss://destination@datalake1558.dfs.core.windows.net/spotify')
-- MAGIC
-- MAGIC # Remove duplicates
-- MAGIC # df_deduplicated = df_delta.dropDuplicates(subset=['spotify_id','country'])
-- MAGIC # repartition
-- MAGIC df_deduplicated.repartition(400).write.format('delta')\
-- MAGIC     .mode('overwrite')\
-- MAGIC     .save('abfss://destination@datalake1558.dfs.core.windows.net/spotify')
-- MAGIC # Overwrite the delta path with deduplicated data
-- MAGIC # df_deduplicated.write.format('delta')\
-- MAGIC #     .mode('overwrite')\
-- MAGIC #     .save('abfss://destination@datalake1558.dfs.core.windows.net/spotify')

-- COMMAND ----------

OPTIMIZE DELTA.`abfss://destination@datalake1558.dfs.core.windows.net/spotify`; --ZORDER BY (Item_Identifier);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed VS External Delta Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Database**

-- COMMAND ----------

CREATE DATABASE salesDB;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Managed Table**

-- COMMAND ----------

CREATE TABLE salesDB.mantable  
USING DELTA  
select * from csv.`/Volumes/man_cata/man_schema/extvolume/sales/`

-- COMMAND ----------

INSERT INTO salesDB.mantable 
VALUES
(1,'aa',30),
(2,'bb',33),
(3,'cc',35),
(4,'DD',40)

-- COMMAND ----------

select * from salesDB.mantable;

-- COMMAND ----------

describe extended salesDB.mantable

-- COMMAND ----------

describe extended salesDB.mantable

-- COMMAND ----------

DROP TABLE salesDB.mantable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **External Table**

-- COMMAND ----------

CREATE EXTERNAL LOCATION my_external_location
URL 'abfss://destination@datalake1558.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL external_location_cred)

-- COMMAND ----------

CREATE TABLE salesDB.exttable  
(
  id INT,
  name STRING,
  marks INT 
)
USING DELTA    
LOCATION 'abfss://destination@datalake1558.dfs.core.windows.net/salesDB/exttable';

-- COMMAND ----------

INSERT INTO salesDB.exttable 
VALUES
(1,'aa',30),
(2,'bb',33),
(3,'cc',35),
(4,'DD',40)

-- COMMAND ----------

select * from salesDB.exttable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Tables Functionalities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **INSERT**

-- COMMAND ----------

INSERT INTO salesDB.exttable 
VALUES
(5,'aa',30),
(6,'bb',33),
(7,'cc',35),
(8,'DD',40)

-- COMMAND ----------

select * from salesdb.exttable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **DELETE**

-- COMMAND ----------

DELETE FROM salesdb.exttable 
WHERE id = 8

-- COMMAND ----------

select * from salesdb.exttable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **DATA VERSIONING**

-- COMMAND ----------

DESCRIBE HISTORY salesdb.exttable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **TIME TRAVEL**

-- COMMAND ----------

select * from salesdb.exttable version as of 6;

-- COMMAND ----------

RESTORE TABLE salesdb.exttable TO VERSION AS OF 2;

-- COMMAND ----------

select * from salesDB.exttable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **VACUUM**

-- COMMAND ----------

VACUUM salesdb.exttable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **VACUUM RETAIN 0 HRS**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC parquet files 1ee and c11 will remain, c09 gets deleted

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM salesdb.exttable RETAIN 1 HOURS ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELTA Table Optimization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **OPTIMIZE**

-- COMMAND ----------

OPTIMIZE salesDB.exttable

-- COMMAND ----------

select * from salesdb.exttable

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **ZORDER BY**

-- COMMAND ----------

OPTIMIZE salesdb.exttable ZORDER BY (id)

-- COMMAND ----------

select * from salesdb.exttable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### AUTO LOADER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Streaming Dataframe**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.readStream.format('cloudFiles')\
-- MAGIC         .option('cloudFiles.format','parquet')\
-- MAGIC         .option('cloudFiles.schemaLocation','abfss://aldestination@datalake1558.dfs.core.windows.net/checkpoint')\
-- MAGIC         .load('abfss://alsource@datalake1558.dfs.core.windows.net')   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.writeStream.format('delta')\
-- MAGIC                .option('checkpointLocation','abfss://aldestination@datalake1558.dfs.core.windows.net/checkpoint')\
-- MAGIC                .option('mergeSchema','true')\
-- MAGIC                .trigger(processingTime='5 seconds')\
-- MAGIC                .start('abfss://aldestination@datalake1558.dfs.core.windows.net/data')
