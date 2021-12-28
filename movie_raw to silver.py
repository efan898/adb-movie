# Databricks notebook source
# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest raw data

# COMMAND ----------

file_paths = [file.path for file in dbutils.fs.ls("dbfs:/FileStore/tables/") if "movie_" in file.path]

# COMMAND ----------

print(file_paths)

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest Raw

# COMMAND ----------

from pyspark.sql.functions import *

rawDF = (spark.read
         .option("multiline", "true")
         .format("json")
         .load(file_paths)
         .select(explode("movie").alias("movies")))

# COMMAND ----------

display (rawDF)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

rawDF = rawDF.select(
    "movies",
    lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
)

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Raw to Bronze

# COMMAND ----------

from pyspark.sql.functions import col

(
    rawDF.select(
        "datasource",
        "ingesttime",
        "movies",
        "status"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(bronzePath2)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath2))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath2}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze to Silver

# COMMAND ----------

silverDF = bronzeDF.select(
                           "movies.Id", 
                           "movies.Title", 
                           "movies.Overview",
                           "movies.Budget",
                           "movies.Revenue",
                           "movies.Runtime",
                           "movies.Price",
                           "movies"
                       
                          )

# COMMAND ----------

display(silverDF)

# COMMAND ----------

from pyspark.sql.functions import col

silverDF = silverDF.select(
                           col("Id").cast("integer").alias("movie_id"),
                           "Title", 
                           "Overview",
                           col("Budget").cast("integer"),
                           col("Revenue").cast("integer"),
                           col("RunTime").cast("integer"),
                           col("Price").cast("float"),
                           "movies"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Quarantine the Bad Data

# COMMAND ----------

silverDF.count()

# COMMAND ----------

silverDF.na.drop().count()

# COMMAND ----------

silverDF=silverDF.drop_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Split the Silver DataFrame

# COMMAND ----------

silverDF_clean=silverDF.filter("RunTime >= 0")
silverDF_quarantine=silverDF.filter("RunTime < 0")

# COMMAND ----------

display(silverDF_clean)

# COMMAND ----------

display(silverDF_quarantine)

# COMMAND ----------

(
    silverDF_clean.select(
        "movie_id", "Title", "Overview", "Budget", "Revenue", "RunTime", "Price", "movies"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silverDF
"""
)

spark.sql(
    f"""
CREATE TABLE silverDF
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM silverDF
# MAGIC ORDER BY movie_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Update Bronze table

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath2)
silverAugmented = silverDF_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movies = clean.movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

silverAugmented = silverDF_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movies = quarantine.movies"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

# MAGIC %md
# MAGIC Silver Update

# COMMAND ----------

bronzeQuarantinedDF = spark.read.table("movie_bronze").filter(
    "status = 'quarantined'"
)

display(bronzeQuarantinedDF)

# COMMAND ----------

silverQuarantinedDF = bronzeQuarantinedDF.select(
                                                 "movies.Id", 
                                                 "movies.Title", 
                                                 "movies.Overview",
                                                 "movies.Budget",
                                                 "movies.Revenue",
                                                 "movies.Runtime",
                                                 "movies.Price",
                                                 "movies"
                       )

# COMMAND ----------

display(silverQuarantinedDF)

# COMMAND ----------

from pyspark.sql.functions import col

silverQuarantinedDF = silverQuarantinedDF.select(
                           col("Id").cast("integer").alias("movie_id"),
                           "Title", 
                           "Overview",
                           col("Budget").cast("integer"),
                           col("Revenue").cast("integer"),
                           abs(col("RunTime")).cast("integer").alias("Runtime"),
                           col("Price").cast("float"),
                           "movies"
)

# COMMAND ----------

display(silverQuarantinedDF)

# COMMAND ----------

from pyspark.sql.functions import col

(
    silverQuarantinedDF.select("movie_id", "Title", "Overview", "Budget", "Revenue", "RunTime", "Price", "movies"
    )
    .write.format("delta")
    .mode("append")
    .save(silverPath)
)

# COMMAND ----------

display(silverDF)

# COMMAND ----------

from pyspark.sql.functions import *

silverTable = (spark.read
             
             .load(silverPath)
         )

# COMMAND ----------

display(silverTable)
