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

budget_silver = bronzeDF.select("movies.Id",
                                "movies.Title",
                                "movies.budget",
                                "movies"
                               )
display(budget_silver)

# COMMAND ----------

from pyspark.sql.functions import col

budget_silver = budget_silver.select(col("Id").cast("integer").alias("movie_id"),
                                     col("Title").alias("movie_title"),
                                     col("budget").cast("integer").alias("budget"),
                                     "movies"
                                    )

# COMMAND ----------

budget_silver.count()

# COMMAND ----------

budget_silver = budget_silver.drop_duplicates()

# COMMAND ----------

budget_silver.count()

# COMMAND ----------

budgetSilverClean=budget_silver.filter("budget >= 1000000")
budgetSilverQuarantine=budget_silver.filter("budget < 1000000")

# COMMAND ----------

budgetSilverClean.count()

# COMMAND ----------

budgetSilverQuarantine.count()

# COMMAND ----------

(
    budgetSilverClean.select(
        "movie_id", "movie_title", "budget", "movies"
    )
    .write.format("delta")
    .mode("append")
    .save(budgetSilverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS budgetSilver
"""
)

spark.sql(
    f"""
CREATE TABLE budgetSilver
USING DELTA
LOCATION "{budgetSilverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from budgetSilver
# MAGIC order by movie_id ASC

# COMMAND ----------

(
    budgetSilverQuarantine.select(
        "movie_id", "movie_title", "budget", "movies"
    )
    .write.format("delta")
    .mode("append")
    .save(budgetFixPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS budgetFix
"""
)

spark.sql(
    f"""
CREATE TABLE budgetFix
USING DELTA
LOCATION "{budgetFixPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from budgetFix
# MAGIC order by movie_id ASC

# COMMAND ----------

budgetAugmented = budgetSilverQuarantine.withColumn("budget", lit("1000000"))

display(budgetAugmented)

# COMMAND ----------

from pyspark.sql.functions import col

(
    budgetAugmented.select("movie_id", 
                           "movie_title", 
                           col("budget").cast("integer").alias("budget"), 
                           "movies"
    )
    .write.format("delta")
    .mode("append")
    .save(budgetSilverPath)
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from budgetSilver
# MAGIC order by movie_id ASC
