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
		//when row contains value
		if (row.getAs[JBigDecimal](0) != null) {
			val openPrice = row.getAs[JBigDecimal](0)
			val openPriceTs = row.getAs[Timestamp](1)
			val closePrice = row.getAs[JBigDecimal](2)
			val closePriceTs = row.getAs[Timestamp](3)

			//if buffer contains values
			if (buffer.getAs[Timestamp](1) != null){
				val ots = buffer.getAs[Timestamp](1)
				//get min time
				if (openPriceTs.before(ots)) {
					buffer(0) = openPrice
					buffer(1) = openPriceTs
				}
				val cts = buffer.getAs[Timestamp](3)
				//get the max time
				if (closePriceTs.after(cts)) {
					buffer(2) = closePrice
					buffer(3) = closePriceTs
				}
			} else {
				buffer(0) = openPrice
				buffer(1) = openPriceTs
				buffer(2) = closePrice
				buffer(3) = closePriceTs
			}
		}
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
}