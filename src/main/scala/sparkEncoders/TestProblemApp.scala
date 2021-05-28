package sparkEncoders

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{ Dataset, SparkSession }

/** Simple executable to reproduce the problem from a spark job */
object TestProblemApp extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("TestProblemApp")
    .getOrCreate()

  import spark.implicits._
  import problem._

  val input                 = Seq(1, 2)
  val dataset: Dataset[Int] = spark.createDataset(input)

  dataset.withColumnDataset[String](lit("test")).show(false)

}
