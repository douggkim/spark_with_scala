package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, stddev, sum}

object Aggregations extends App{
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")).show() // count all rows, and will include null

  //count Distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) //appromxiate counting, for large datasets

  // min and max
  moviesDF.select(functions.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("max(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")).as("Avg_rotten_tomatoes_rating"),
    stddev(col("Rotten_Tomatoes_Rating")).as("stddev_rotten_tomatoes_rating")
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count()

  val avgRatingByGenreD = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  //multiple aggregations
  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last) //.orderBy(col("Avg_Rating").desc) -> for descending
  //.orderBy(col("Avg_Rating").desc_nulls_last) -> for descending

  aggregationsByGenreDF.show()



}
