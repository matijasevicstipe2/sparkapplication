import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark 2 Recruitment Challenge")
      .config("spark.driver.host", "localhost")
      .master("local[*]")
      .getOrCreate()

    //part 1
    var df_reviews = spark
      .read
      .option("header", "true")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .csv("src/main/scala/resources/googleplaystore_user_reviews.csv")

    df_reviews = df_reviews
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))
    df_reviews = df_reviews
      .na.fill(0, Seq("Sentiment_Polarity"))

    var df_1 = df_reviews
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1 = df_1.na.fill(0, Seq("Average_Sentiment_Polarity"))

    df_1.printSchema()
    df_1.show()

    //part 2
    var df_apps = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED")
      .csv("src/main/scala/resources/googleplaystore.csv")

    df_apps = df_apps
      .withColumn("Rating", col("Rating").cast(DoubleType))
    df_apps = df_apps
      .na.fill(0, Seq("Rating"))

    val df_2 = df_apps
      .where("Rating > 4.0").orderBy(desc("Rating"))

    df_2.show()

    df_2.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .option("delimiter", "§")
      .save("/src/main/scala/resources/best_apps")


    //part 3
    df_apps = df_apps
      .withColumn("Reviews", coalesce(col("Reviews").cast(LongType), lit(0)))
      .withColumn("size_value", regexp_extract(col("Size"), "(\\d+\\.?\\d*)", 1).cast(DoubleType))
      .withColumn("size_unit", regexp_extract(col("Size"), "([BKMG])", 1))
      .withColumn("Size",
        when(col("size_unit") === "B", col("size_value") / 1024 / 1024)
          .when(col("size_unit") === "K", col("size_value") / 1024)
          .when(col("size_unit") === "G", col("size_value") * 1024)
          .otherwise(col("size_value"))
      )
      .drop("size_value", "size_unit")
      .withColumn("Price", col("Price").cast(DoubleType) * 0.9)
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Last_Updated", to_date(col("Last Updated"), "MMMM d, yyyy"))
      .drop("Last Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")


    val window = Window.partitionBy(col("App")).orderBy(col("Reviews").desc)

    val dfWithMaxReviews = df_apps
      .withColumn("max_reviews", max(col("Reviews")).over(window))

    dfWithMaxReviews.show()

    val dfWithCategoriesArray = dfWithMaxReviews
      .groupBy(col("App"))
      .agg(array_distinct(collect_list("Category")).as("Categories"))

    dfWithCategoriesArray.show()

    val df_3 = dfWithCategoriesArray
      .join(dfWithMaxReviews, Seq("App"), "inner")
      .where(col("Reviews") === col("max_reviews"))
      .drop("max_reviews", "Category")

    df_3.printSchema()
    df_3.show()

    //part 4
    val df_4 = df_3
      .join(df_1, Seq("App"), "left")

    df_4.show()

    df_4.write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_cleaned")


    //part 5
    val df_5 = df_4
      .withColumn("Genre", explode(col("Genres")))
      .groupBy(col("Genre"))
      .agg(
        count("*").as("Count"),
        avg(col("Rating")).as("Average_Rating"),
        avg(col("Average_Sentiment_Polarity")).as("Average_Sentiment_Polarity"))

    df_5.show()

    df_5.write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_metrics")

  }
}