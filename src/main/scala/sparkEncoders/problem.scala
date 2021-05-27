package sparkEncoders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{ encoderFor, ExpressionEncoder }
import org.apache.spark.sql.functions.{ col, struct }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Column, Dataset, Encoder }

import java.util.UUID
import scala.reflect.runtime.universe.TypeTag

/** Various Spark utility decorators */
object problem {

  def schemaFor[T: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  implicit class DatasetOps[T: Encoder](val dataset: Dataset[T]) extends Serializable {

    /**
     * Add a column to a dataset resulting in a dataset of a tuple of the type contained in the input dataset and the new column
     * @param column the column to be added
     * @tparam U The type of the added column
     * @return a Dataset containing a tuple of the input data and the given column
     */
    def withColumnDataset[U: Encoder](column: Column): Dataset[(T, U)] = {
      implicit val tuple2Encoder: Encoder[(T, U)] = ExpressionEncoder.tuple(encoderFor[T], encoderFor[U])
      val tempColName                             = s"udf_temp_${UUID.randomUUID()}"
      dataset
        .withColumn(tempColName, column)
        .select(struct(dataset.columns.map(col): _*) as "_1", col(tempColName) as "_2")
        .as[(T, U)]
    }
  }

  implicit class KeyValueDatasetOps[K: Encoder, V: Encoder](val dataset: Dataset[(K, V)]) extends Serializable {

    /** Map values of a Dataset containing a Tuple2 */
    def mapValues[U: Encoder](f: V => U): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = ExpressionEncoder.tuple(encoderFor[K], encoderFor[U])
      dataset.map { case (k, v) => (k, f(v)) }
    }

    /** FlatMap values of a Dataset containing a Tuple2 */
    def flatMapValues[U: Encoder](f: V => TraversableOnce[U]): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = ExpressionEncoder.tuple(encoderFor[K], encoderFor[U])
      dataset.flatMap { case (k, v) => f(v).map((k, _)) }
    }
  }
}
