package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()


  val carSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  Reading a DF:
  - format
  - schema or inferSchema = true
  - zero or more options
   */

  //options - mode failFast: will fail if malformed  / other options- dropMalformed, permissive
  //options - path: the path of the source
  // reading a dataframe
  val carsDF = spark.read
    .format("json")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // corrupt the json file to see what's wrong
  // it won't fail without the failFast mode
//  carsDF.take(10).foreach(println)

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()
  /*
  Writing DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists (error when the file already exists)
  - path
  - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path","src/main/resources/data/cars_dupe.json")
    .save()


  spark.read
    .format("json")
    .schema(carSchema)
    .option("dateFormat","YYYY-MM-dd") //dateFormat only works with enforced schema ; if spark fails parking, it will put null
    .option("allowSingleQuotes", "true") // when treating jsons with single quotes
    .option("compression", "uncompressed") // default value = uncompressed, others: bzip2, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/cars.json") // you can also just say .json instead of load

  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  )

  )
  var stocksDF = spark.read
    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY") //dateformat will be different here
    .option("header", "true")
    .option("sep", ",") // it could be semicolon, tab so on ...
    .option("nullValue", "") // tell spark which is the null value
    .load("src/main/resources/data/stocks.csv")

//  stocksDF.show(10)

  //parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .format("parquet") // could be omitted as default is parquet
    .save("src/main/resources/data/cars.parquet")

  //txt
  spark.read
    .text("src/main/resources/data/sample_text.txt")
    .show()

  // Reading from a remote database
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show(10)

  /*
   Exercise: read the movies DF, then write it as
   - tab-separated values file
   - snappy parquet
   - table public.movies in the postgresql
   */
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .load("src/main/resources/data/movies.json")

  // Write as tab-separated values file
  moviesDF.write
    .format("csv")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.tsv")

  moviesDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("compression","snappy")
    .save("src/main/resources/data/movies.parquet")

  // Write as tab-separated values file
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()







}
