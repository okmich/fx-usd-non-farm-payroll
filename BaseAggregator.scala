import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DecimalType,StructType}

import java.math.{BigDecimal => JBigDecimal}

abstract class BaseAggregator(colName: String) extends UserDefinedAggregateFunction {

  type Price = (JBigDecimal, Timestamp)

  override def inputSchema: StructType = StructType(
    StructField("day", StringType) ::
    StructField("hour", IntegerType) ::
    StructField("min", IntegerType) ::
    StructField("sec", IntegerType) ::
    StructField("milli", IntegerType) ::
    StructField("tradHour", IntegerType) ::
    StructField("tradMin", IntegerType) ::
    StructField("price", new DecimalType(12,6)) ::
    StructField("thresh", IntegerType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("price", new DecimalType(12,6)) ::
    StructField("priceTs", TimestampType) :: Nil)

  override def dataType: DataType = StructType(
    StructField(colName, new DecimalType(12,6)) ::
    StructField(colName + "_ts", TimestampType) :: Nil)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = null
      buffer(1) = null
    }

  override def deterministic : Boolean = true

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val day = input.getAs[String](0)
    val hour = input.getAs[Int](1)
    val min = input.getAs[Int](2)
    val sec = input.getAs[Int](3)
    val milli = input.getAs[Int](4)
    val tradHour = input.getAs[Int](5)
    val tradMin = input.getAs[Int](6)
    val price = input.getAs[JBigDecimal](7)
    val x = input.getAs[Int](8)

    val nfpTs = getNfpTs(day, tradHour, tradMin)
    val tickTs = getTickTs(day, hour, min, sec, milli)
    //nfpTsPlusThresh
    val maxTs = tsPlusMins(nfpTs, x)
    //if the tickTs is between nfpTs and maxTs
    //big filtering 
    println(s"nfpTs is $nfpTs while tickTs is $tickTs")
    if (tickTs.after(nfpTs) && tickTs.before(maxTs)) {
      buffer(0) = price
      buffer(1) = tickTs
    }
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    val result = getPricePoint(buffer, row)

    buffer(0) = result._1
    buffer(1) = result._2
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    (buffer(0), buffer(1))
  }

  def getTickTs(day: String, hour: Int, min: Int, sec: Int, milli:Int) : Timestamp  = {
    new Timestamp(new LocalDateTime(
      day.substring(0,4).toInt,
      day.substring(4,6).toInt,
      day.substring(6,8).toInt,
      hour, min, sec, milli
    ).toDate.getTime)
  }

  def getPricePoint(a : Price, b: Price): Price

  def getNfpTs(day: String, nfpHour: Int, nfpMin: Int) : Timestamp = {
    new Timestamp(new LocalDateTime(
      day.substring(0,4).toInt,
      day.substring(4,6).toInt,
      day.substring(6,8).toInt,
      nfpHour, nfpMin, 0, 0
    ).toDate.getTime)
  } 

  def tsPlusMins(ts: Timestamp, thresh: Int) : Timestamp = {
    val localDT = new LocalDateTime(ts.getTime).plusMinutes(thresh)

    new Timestamp(localDT.toDate.getTime)
  } 

  // def getEarliestAndLatest(a : Price, b: Price) : Price = {
  //   var open : (JBigDecimal, Timestamp) = null
  //   var close : (JBigDecimal, Timestamp) = null

  //   val tickPrice = b._1
  //   val tickTs = b._2

  //   if (tickTs != null){
  //     //get min time
  //     open = if (a._2 == null || tickTs.before(a._2)) (tickPrice, tickTs) else (a._1, a._2)
  //     //get the max time
  //     close = if (a._2 == null || tickTs.after(a._4)) (tickPrice, tickTs) else (a._3, a._4)
  //   } else {
  //     open = (a._1, a._2)
  //     close = (a._3, a._4)
  //   }

  //   (open._1, open._2, close._1, close._2)
  // }

  implicit def castInternalBufferToPrice(buffer: MutableAggregationBuffer) : Price = {
    (buffer.getAs[JBigDecimal](0), buffer.getAs[Timestamp](1))
  }

  implicit def castInternalBufferToPrice(row: Row) : Price = {
    (row.getAs[JBigDecimal](0), row.getAs[Timestamp](1))
  }
}


class FirstAggregator extends BaseAggregator("open_price") {
  def getPricePoint(a : Price, b: Price) : Price = {
    if (a != null) a else b
  }
}


class OpenAggregator extends BaseAggregator("open_price") {
  def getPricePoint(a : Price, b: Price) : Price = {
    var open : (JBigDecimal, Timestamp) = null

    val tickPrice = b._1
    val tickTs = b._2

    if (tickTs != null){
      //get min time
      open = if (a._2 == null || tickTs.before(a._2)) (tickPrice, tickTs) else (a._1, a._2)
    } else {
      open = (a._1, a._2)
    }

    (open._1, open._2)  
  }
}

class CloseAggregator extends BaseAggregator("close_price") {
  def getPricePoint(a : Price, b: Price) : Price = {
    var close : (JBigDecimal, Timestamp) = null

    val tickPrice = b._1
    val tickTs = b._2

    if (tickTs != null){
      //get max time
      close = if (a._2 == null || tickTs.after(a._2)) (tickPrice, tickTs) else (a._1, a._2)
    } else {
      close = (a._1, a._2)
    }
    (close._1, close._2)  
  }
}

class MaxPriceAggregator extends BaseAggregator("high_price") {
  def getPricePoint(a : Price, b: Price) : Price = {
    var max : (JBigDecimal, Timestamp) = null

    val tickPrice = b._1
    val tickTs = b._2

    if (tickPrice != null){
      //get max price
      max = if (a._1 == null || (tickPrice.compareTo(a._1)  >= 0)) (tickPrice, tickTs) else (a._1, a._2)
    } else {
      max = (a._1, a._2)
    }
    (max._1, max._2)  
  }
}