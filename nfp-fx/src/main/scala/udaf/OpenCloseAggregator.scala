package udaf

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Timestamp

import org.joda.time.LocalDateTime

class OpenCloseAggregator extends UserDefinedAggregateFunction {

	type Price = (JBigDecimal, Timestamp, JBigDecimal, Timestamp)

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
	    StructField("oPrice", new DecimalType(12,6)) ::
	    StructField("oPriceTs", TimestampType) ::
	    StructField("cPrice", new DecimalType(12,6)) ::
	    StructField("cPriceTs", TimestampType) :: Nil)

   	override def dataType: DataType = StructType(
	    StructField("open_price", new DecimalType(12,6)) ::
	    StructField("open_price_ts", TimestampType) ::
	    StructField("close_price", new DecimalType(12,6)) ::
	    StructField("close_price_ts", TimestampType) :: Nil)

	override def initialize(buffer: MutableAggregationBuffer): Unit = {
	    buffer(0) = null
	    buffer(1) = null
	    buffer(2) = null
	    buffer(3) = null
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
		//if the tickTs is between nfpTs and nfpTsPlusThresh
		//big filtering 
		if (tickTs.after(nfpTs) && tickTs.before(tsPlusMins(nfpTs, x))) {
			//println(s"nfpTs is $nfpTs while tickTs is $tickTs")
			buffer(0) = price
			buffer(1) = tickTs
			buffer(2) = price
			buffer(3) = tickTs
		}
	}

	override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
		println("Merge " + (buffer(0),buffer(1),buffer(2),buffer(3)))
		val result = getEarliestAndLatest(buffer, row)

		buffer(0) = result._1
		buffer(1) = result._2
		buffer(2) = result._3
		buffer(3) = result._4
	}

	// This is where you output the final value, given the final value of your bufferSchema.
	override def evaluate(buffer: Row): Any = {
		(buffer(0), buffer(1), buffer(2), buffer(3))
	}

	def getTickTs(day: String, hour: Int, min: Int, sec: Int, milli:Int) : Timestamp  = {
		new Timestamp(new LocalDateTime(
			day.substring(0,4).toInt,
			day.substring(4,6).toInt,
			day.substring(6,8).toInt,
			hour, min, sec, milli
		).toDate.getTime)
	}

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

	def getEarliestAndLatest(a : Price, b: Price) : Price = {
		var open : (JBigDecimal, Timestamp) = null
		var close : (JBigDecimal, Timestamp) = null

		if (b._2 != null){
			//get min time
			open = if (a._2 == null || b._2.before(a._2)) (b._1, b._2) else (a._1, a._2)
			//get the max time
			close = if (a._2 == null || b._4.after(a._4)) (b._3, b._4) else (a._3, a._4)
		} else {
			open = (a._1, a._2)
			close = (a._3, a._4)
		}

		(open._1, open._2, close._1, close._2)
	}

	implicit def castInternalBufferToPrice(buffer: MutableAggregationBuffer) : Price = {
		(buffer.getAs[JBigDecimal](0), buffer.getAs[Timestamp](1),
			 buffer.getAs[JBigDecimal](2), buffer.getAs[Timestamp](3))
	}

	implicit def castInternalBufferToPrice(row: Row) : Price = {
		(row.getAs[JBigDecimal](0), row.getAs[Timestamp](1), 
			row.getAs[JBigDecimal](2), row.getAs[Timestamp](3))
	}
}