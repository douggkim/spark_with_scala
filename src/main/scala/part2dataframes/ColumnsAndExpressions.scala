package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import part2dataframes.DataSources.spark

object ColumnsAndExpressions extends App{
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name") //result is JVM object called column / empty for now

  // selecting (Projection)
  val carNamesDF = carsDF.select(firstColumn) // a new DataFrame is only created when it's selected
  carNamesDF.show()

  //various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol ,auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a column object
    expr("Origin") // EXPRESSION
  )
  // select with plain column names
  carsDF.select("Name","Year")

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs/2.2").as("Weight_in_kg_2")
  )
  carsWithWeightsDF.show()
  //selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs")/2.2)
  //renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsWithColumnRenamed.show()
  //careful with column names
//  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  //remove a column
  carsWithColumnRenamed.drop("Cylinders","Displacement")
  //filtering  - also can use where
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  //chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin")=== "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin")=== "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = more cars
  val moreCarsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

}
