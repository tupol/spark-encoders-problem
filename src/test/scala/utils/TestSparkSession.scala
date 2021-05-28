package utils
import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, Suite }

/**
 */
trait TestSparkSession extends BeforeAndAfterAll { this: Suite =>

  def appName           = "Test"
  def driverBindAddress = "127.0.0.1"

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master("local[*]")
    .config("spark.driver.bindAddress", driverBindAddress)
    .getOrCreate()

}
