import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DecimalType,StructType}

import java.math.{BigDecimal => JBigDecimal}

class OHLCAggregator extends UserDefinedAggregateFunction {

  override def inputSchema: StructType =
    StructType(StructField("value", new DecimalType(12,6)) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("open", new DecimalType(12,6)) ::
    StructField("high", new DecimalType(12,6)) ::
    StructField("low", new DecimalType(12,6)) ::
    StructField("close", new DecimalType(12,6)) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    StructField("open", new DecimalType(12,6)) ::
    StructField("high", new DecimalType(12,6)) ::
    StructField("low", new DecimalType(12,6)) ::
    StructField("close", new DecimalType(12,6)) :: Nil
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = null
    buffer(2) = null
    buffer(3) = null
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val price = input.getAs[JBigDecimal](0)

    buffer(0) = if (buffer.getAs[JBigDecimal](0) == null) 
                    price 
                else buffer.getAs[JBigDecimal](0)
    buffer(1) = if (buffer.getAs[JBigDecimal](1) == null || buffer.getAs[JBigDecimal](1).compareTo(price) <= 0)  
                    price 
                else buffer.getAs[JBigDecimal](1)
    buffer(2) = if (buffer.getAs[JBigDecimal](2) == null || buffer.getAs[JBigDecimal](2).compareTo(price) >= 0) 
                    price 
                else buffer.getAs[JBigDecimal](2)
    buffer(3) = price
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = if (buffer1.getAs[JBigDecimal](0) == null) 
                    buffer2.getAs[JBigDecimal](0) 
                else buffer1.getAs[JBigDecimal](0)
    buffer1(1) = if (buffer1.getAs[JBigDecimal](1) == null || buffer1.getAs[JBigDecimal](1).compareTo(buffer2.getAs[JBigDecimal](1)) <= 0)  
                    buffer2.getAs[JBigDecimal](1) 
                else buffer1.getAs[JBigDecimal](1)
    buffer1(2) = if (buffer1.getAs[JBigDecimal](2) == null || buffer1.getAs[JBigDecimal](2).compareTo(buffer2.getAs[JBigDecimal](2)) >= 0)  
                    buffer2.getAs[JBigDecimal](2) 
                else buffer1.getAs[JBigDecimal](2)
    buffer1(3) = buffer2.getAs[JBigDecimal](3)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    (buffer(0),buffer(1),buffer(2),buffer(3))
  }
}
