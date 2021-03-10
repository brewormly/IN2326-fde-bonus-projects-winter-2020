import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

// my imports
//import math._

object ReturnTrips {

  /*
    Haversine code is taken from https://rosettacode.org/wiki/Haversine_formula#Scala
    I skip the conversion to radians, because coordinates are already radians...
    And we need only double values and return value in m!
   */

  // radius of the earth in km which gives the correct result...
  // also tried 6372.8 km and failed...
  val R = 6371.0
  /*
    I am not that familiar with Scala and Spark, so here is the reason why I use UDF:
    https://medium.com/@talentorigin/spark-udf-user-defined-function-using-scala-approach-1-be2463867528
   */
  val haversine = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    import scala.math._
    val dLat = (lat2 - lat1)
    val dLon = (lon2 - lon1)

    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1) * cos(lat2)
    val c: Double = 2 * asin(sqrt(a))
    R * c * 1000
  }

  /*
    Converting degrees to radians
   */
  def convertRadians = (degree: Double) => {
    import scala.math._
    degree * Pi / 180
  }

  /*
    We have to convert 8 hours into seconds...
   */
  val maxTimeBetweenRides = 8 * 60 * 60


  def compute(trips: Dataset[Row], dist: Double, spark: SparkSession): Dataset[Row] = {
    // we have four cores on the gitlab ci test pipeline
    import spark.implicits._
    spark.conf.set("spark.default.parallelism", 4)

    // use udfs
    def distance = udf(haversine)
    def convertToRadians = udf(convertRadians)

    /*
      We need some new columns and we also have to convert them to radians...
      The timestamp is converted to seconds as John suggested.
     */
    val preprocessedTrips = trips.withColumn(
      "pickup_latitude_first",
      convertToRadians('pickup_latitude)).withColumn("pickup_longitude_first",
      convertToRadians('pickup_longitude)).drop("pickup_longitude").withColumn("dropoff_longitude_first",
      convertToRadians('dropoff_longitude)).withColumn("dropoff_latitude_first",
      convertToRadians('dropoff_latitude)).withColumn("pickup_time", unix_timestamp($"tpep_pickup_datetime")).withColumn("dropoff_time", unix_timestamp($"tpep_dropoff_datetime")
    )

    // select most important columns first
    val tripsSubset = preprocessedTrips.select("pickup_time", "dropoff_time", "pickup_longitude_first", "pickup_latitude_first", "dropoff_longitude_first", "dropoff_latitude_first")

    // calculate buckets again thanks to John...
    val tripsSubsetCalc = tripsSubset
      .withColumn("pickup_time_bucket", floor($"pickup_time" / maxTimeBetweenRides))
      .withColumn("dropoff_time_bucket", floor($"dropoff_time" / maxTimeBetweenRides))
      .withColumn("pickup_latitude_second", floor($"pickup_latitude_first" / (lit(dist) / (R * 1000))))
      .withColumn("dropoff_latitude_second", floor($"dropoff_latitude_first" / (lit(dist) / (R * 1000))))

    // applying this great exploding thingy...
    // is caching better here?
    val tripsBucket = broadcast(tripsSubsetCalc
      .withColumn("dropoff_time_bucket", explode(array($"dropoff_time_bucket", $"dropoff_time_bucket" + 1)))
      .withColumn("pickup_latitude_second", explode(array($"pickup_latitude_second" - 1, $"pickup_latitude_second", $"pickup_latitude_second" + 1)))
      .withColumn("dropoff_latitude_second", explode(array($"dropoff_latitude_second" - 1, $"dropoff_latitude_second", $"dropoff_latitude_second" + 1))))
      .cache()

    /*
        val chosen_data2 = chosen_data1.
    withColumn("dropoff_time", explode(array($"dropoff_time", $"dropoff_time"+1))).
    withColumn("pickup_lat", explode(array($"pickup_lat"-1, $"pickup_lat", $"pickup_lat"+1))).
    withColumn("dropoff_lat", explode(array($"dropoff_lat"-1, $"dropoff_lat", $"dropoff_lat"+1)))



     */


    // let's join those bloody, dirty buckets
    val tripsJoin = tripsBucket.as("first")
      .join(
        tripsSubsetCalc.as("second"),
        ($"first.pickup_latitude_second" === $"second.dropoff_latitude_second")
          && ($"first.dropoff_latitude_second" === $"second.pickup_latitude_second")
          && ($"first.dropoff_time_bucket" === $"second.pickup_time_bucket")
      )

    // Applying the constraints from the exercise sheet...
    val finalTrips = tripsJoin
      .filter(
        distance($"first.dropoff_latitude_first", $"first.dropoff_longitude_first", $"second.pickup_latitude_first", $"second.pickup_longitude_first") < lit(dist)
          &&
          distance($"second.dropoff_latitude_first", $"second.dropoff_longitude_first", $"first.pickup_latitude_first", $"first.pickup_longitude_first") < lit(dist))
      .filter(($"first.dropoff_time" + maxTimeBetweenRides) > $"second.pickup_time")
      .filter($"first.dropoff_time" < $"second.pickup_time")

    finalTrips

  }
}
