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
     * @tparam U The type of the added column; this can not be inferred, so it MUST be specified
     * @return a Dataset containing a tuple of the input data and the given column
     */
    def withColumnDataset[U: Encoder](column: Column): Dataset[(T, U)] = {
      implicit val tuple2Encoder: Encoder[(T, U)] = ExpressionEncoder.tuple(encoderFor[T], encoderFor[U])
      val tempColName                             = s"udf_temp_${UUID.randomUUID()}"

      val tuple1 =
        if (dataset.encoder.clsTag.runtimeClass.isPrimitive) dataset.columns.map(col).head
        else struct(dataset.columns.map(col): _*)

      dataset
        .withColumn(tempColName, column)
        .select(tuple1 as "_1", col(tempColName) as "_2")
        .as[(T, U)]
    }
  }

}
