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

genre_silver = bronzeDF.select(explode("movies.genres").alias("genres"), "movies")
genre_silver = genre_silver.select(
                                   "genres.id",
                                   "genres.name",
                                   "movies")

# COMMAND ----------

from pyspark.sql.functions import col

genre_silver = genre_silver.select(col("id").cast("integer").alias("genre_id"),
                                   col("name").alias("genre_name"),
                                   "movies")

# COMMAND ----------

display(genre_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Silver

# COMMAND ----------

(
    genre_silver
    .write.format("delta")
    .mode("overwrite")
    .save(genreSilverPath2)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genre_silver
USING DELTA
LOCATION "{genreSilverPath}"
"""
)

# COMMAND ----------

movie_silver = bronzeDF.select(
                           "movies.Id", 
                           "movies.Title",
                           "movies"
                      )

# COMMAND ----------

from pyspark.sql.functions import col

movie_silver=movie_silver.select(col("Id").cast("integer").alias("movie_id"),
                                 col("Title").alias("movie_title"),
                                 "movies")

# COMMAND ----------

display(movie_silver)

# COMMAND ----------

genre_silver_df = spark.read.table("genre_silver").alias("genre")

joinDF = movie_silver.join(
    genre_silver_df,
    genre_silver_df.movies == movie_silver.movies,
)
display(joinDF)

# COMMAND ----------

from pyspark.sql.functions import col

joinDF = joinDF.select("movie_id",
                       "movie_title",
                       col("id").cast("integer").alias("genre_id"),
                       col("name").alias("genre_name")
                      )

# COMMAND ----------

display(joinDF)

# COMMAND ----------

joinDF.count()

# COMMAND ----------

joinDF=joinDF.drop_duplicates()

# COMMAND ----------

joinDF.count()
