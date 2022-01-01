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
    current_timestamp().cast("date").alias("ingestdate")
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

language_silver = bronzeDF.select("movies.Id",
                                "movies.Title",
                                "movies.OriginalLanguage",
                                "movies"
                               )
display(language_silver)

# COMMAND ----------

from pyspark.sql.functions import col

language_silver = language_silver.select(col("Id").cast("integer").alias("movie_id"),
                                     col("Title").alias("movie_title"),
                                     "OriginalLanguage",
                                     "movies"
                                    )

# COMMAND ----------

language_silver.count()

# COMMAND ----------

language_silver=language_silver.drop_duplicates()

# COMMAND ----------

language_silver.count()

# COMMAND ----------

(
    language_silver.select(
        "movie_id", "movie_title", "OriginalLanguage", "movies"
    )
    .write.format("delta")
    .mode("append")
    .save(languageSilverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS languageSilver
"""
)

spark.sql(
    f"""
CREATE TABLE languageSilver
USING DELTA
LOCATION "{languageSilverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from languageSilver
# MAGIC 
# MAGIC order by movie_id desc
