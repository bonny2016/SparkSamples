package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhaoyugu on 18/04/2017.
  * To submit to spark cluste:
  * spark-submit --class rddSample --master spark://x.x.x.x:7077 SparkSamples_2.11-1.0.jar
  */
object rddSample {

  def wordCount(spark:SparkSession):Unit = {
    val l1 = "Clicking the Refresh Project link in the banner, the little processing bar at the bottom of the screen started churning for a while and I found that all kinds of objects got added to the libraries of my project."
    val l2 = "This is clearly what SBT is meant to do.  I also received some warnings about multiple dependencies with different versions.  I'm still not sure how to address these warnings."
    val lines = List(l1, l2)
    val rdd : RDD[String] = spark.sparkContext.parallelize(lines)
    val counts = rdd.flatMap( line => line.split(" ")).map( (_, 1)).reduceByKey( _ + _)
    counts.collect().foreach(println)

  }
  def forEachVsTake(spark:SparkSession) : Unit = {
    val numbers = Range(1, 10000).toList
    val rdd = spark.sparkContext.parallelize(numbers)
    rdd.foreach(System.out.println)
    rdd.take(100).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    wordCount(spark)

    forEachVsTake(spark)

    spark.stop()
  }

}
