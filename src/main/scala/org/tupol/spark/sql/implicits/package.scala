package org.tupol.spark.sql
import org.apache.spark.sql.{ Column, Dataset, Encoder }
import org.apache.spark.sql.catalyst.encoders.{ encoderFor, ExpressionEncoder }
import org.apache.spark.sql.functions.{ col, struct }

import java.util.UUID

package object implicits {

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
