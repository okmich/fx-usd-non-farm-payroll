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

class TimeFrameAggregator extends UserDefinedAggregateFunction {

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
	    StructField("tickTs", TimestampType) ::
	    StructField("nfpTs", TimestampType) ::
	    StructField("nfpTsPlusThresh", TimestampType) ::
	    StructField("maxPrice", new DecimalType(12,6)) ::
	    StructField("closePrice", new DecimalType(12,6)) ::
	    StructField("maxPriceTs", TimestampType) ::
	    StructField("closePriceTs", TimestampType) :: Nil)

   	override def dataType: DataType = StructType(
	    StructField("max_price", new DecimalType(12,6)) ::
	    StructField("max_price_ts", TimestampType) ::
	    StructField("close_price", new DecimalType(12,6)) ::
	    StructField("close_price_ts", TimestampType) :: Nil)

	override def initialize(buffer: MutableAggregationBuffer): Unit = {
	    buffer(0) = null
	    buffer(1) = null
	    buffer(2) = null
	    buffer(3) = null
	    buffer(4) = null
	    buffer(5) = null
	    buffer(6) = null
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
		val thresh = input.getAs[Int](8)

		val nfpTs = getNfpTs(day, tradHour, tradMin)

		buffer(0) = getTickTs(day, hour, min, sec, milli)
		buffer(1) = nfpTs
		buffer(2) = tsPlusMins(nfpTs, thresh)
		buffer(3) = price
		buffer(4) = price
		buffer(5) = buffer(0)
		buffer(6) = buffer(0)
	}

	// This is how to merge two objects with the bufferSchema type.
	override def merge(buffer1: MutableAggregationBuffer, row: Row): Unit = {
		val tickTs = row.getAs[Timestamp](0)
		val nfpTs = row.getAs[Timestamp](1)
		val nfpTsPlusThresh = row.getAs[Timestamp](1)
		//if the tickTs is between nfpTs and nfpTsPlusThresh
		if (tickTs.after(nfpTs) && tickTs.before(nfpTsPlusThresh)){
			buffer1(0) = tickTs
			buffer1(1) = nfpTs
			buffer1(2) = nfpTsPlusThresh
			//get maxPrice
			val price = buffer1.getAs[JBigDecimal](3)
			val rowPrice = row.getAs[JBigDecimal](3)
			if (rowPrice.compareTo(price) >= 0){
				buffer1(3) = rowPrice
				buffer1(5) = row.getAs[Timestamp](5) //as the tickts as the max
			}
			//get the max time
			val ts = buffer1.getAs[Timestamp](0)
			if (tickTs.after(ts)){
				buffer1(4) = rowPrice
				buffer1(6) = tickTs
			}
		}
	}


	// This is where you output the final value, given the final value of your bufferSchema.
	override def evaluate(buffer: Row): Any = {
		(buffer(3), buffer(5), buffer(4), buffer(6))
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
}