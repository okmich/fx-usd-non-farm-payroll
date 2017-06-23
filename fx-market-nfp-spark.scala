import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.math.{BigDecimal => JBigDecimal}

import java.sql.Timestamp
import java.util.Date


val hiveContext = new HiveContext(sc)

val nfp_hist_df = hiveContext.read.table("fx_nfp.nfp_hist").
	select("trading_day", "trading_hour", "trading_min", "previoius", "forecast", "actual")

val nfp_hist = nfp_hist_df.map((row: Row) => {
	(row.getString(0), row.getString(1).toInt, row.getString(2).toInt, row.getInt(3), row.getInt(4), row.getInt(5))
}).cache

val nfp_hist_schema = nfp_hist_df.schema

val nfp_hist_map = (nfp_hist map (h => (h._1, h))).collect.toMap

val broad_nfp_hist = sc.broadcast(nfp_hist_map)

val tick_data_df = hiveContext.read.table("fx_nfp.tick_data").
	select("day","hour","min","sec","milli","bid","ask","curr_pair")

case class TickData(val day: String, val hour: Int, 
	val min: Int, val sec: Int, val bid: JBigDecimal, 
	val ask:JBigDecimal, val currPair: String) extends java.io.Serializable

case class TickNfpData(val day: String, val hour: Int, 
	val min: Int, val sec: Int, val milli: Int, val bid: JBigDecimal, 
	val ask:JBigDecimal, val currPair: String, val nfpday: String, 
	val nfpHour: Int, val nfpMin: Int, val previous: Int, val forecast: Int, 
	val actual: Int) extends java.io.Serializable

//i added the where clause because of the need to reduce the size of the data
val tick_data_rdd = tick_data_df.where("curr_pair = 'EURUSD'").map((r: Row) => 
	(r.getString(0),r.getString(1).toInt,r.getString(2).toInt,r.getString(3).toInt,
		r.getString(4).toInt,r.getAs[JBigDecimal](5),r.getAs[JBigDecimal](6),r.getString(7)))

val tickNfpDataRdd = tick_data_rdd  mapPartitions (itr => {
	val nfpHistMap = broad_nfp_hist.value
	itr map (tp => {
		 nfpHistMap.get(tp._1) match {
		 	case None => null
		 	case Some(nfp) => TickNfpData(tp._1, tp._2, tp._3, tp._4, tp._5, tp._6, tp._7, tp._8, 
		 		nfp._1, nfp._2, nfp._3, nfp._4, nfp._5, nfp._6)
		 }
	}) filter (_ != null) map (t => (t.day + t.currPair, t))
}) groupByKey
 

val tickDayCurrPairGroupRDD =  tickNfpDataRdd.values.persist(MEMORY_AND_DISK_SER)

import org.joda.time.LocalDateTime


val time = 5

def isWithinXmins(tick: TickNfpData, x: Int) : Boolean = {
	val tradingDt = priceDtFromTickData(tick)
	val reportDt = reportDtFromTickData(tick)
	val afterReport = tradingDt.compareTo(reportDt) > 0
	val beforeReportPlus = tradingDt.compareTo(reportDt.plusMinutes(x)) <= 0

	afterReport && beforeReportPlus
}

val max = tickPrices.filter(isWithinXmins(_, time)).map(t => (t.ask, t.bid)).reduce((a,b) => (a._1.max(b._1), a._2.max(b._2)))

def reduceTickNfpData(p: PriceResult, t: TickNfpData) : PriceResult = {
	//max_ask

	//max_bid

	//close_ask

	//close_bid
}


def aggregateTickGroup(xs: Array[Int], tickGroup : Iterable[TickNfpData]) = {
	xs map (x => {
		tickGroup.filter(isWithinXmins(_, x)).fold(PriceResult.init)(reduceTickNfpData)
		})
}

/**
 *
 */
def newStructTypeForAllTimeframes(xs: Array[Int], schema: StructType): StructType ={
	def createStructTypeForX(x: Int) : StructType ={
		StructType(
			StructField(s"max_ask_$x", new DecimalType(12,6) , false) ::
			StructField(s"max_ask_dt_$x", TimestampType, false) ::
			StructField(s"close_ask_$x", new DecimalType(12,6) , false) ::
			StructField(s"close_ask_dt_$x", TimestampType, false) ::
			StructField(s"max_bid_$x", new DecimalType(12,6) , false) ::
			StructField(s"max_bid_dt_$x", TimestampType, false) ::
			StructField(s"close_bid_$x", new DecimalType(12,6) , false) ::
			StructField(s"close_bid_dt_$x", TimestampType, false) ::
			Nil)
	}
	val structTypes = (xs map createStructTypeForX)
	(structTypes foldLeft schema) ((a,b) => new StructType((a ++ b).toArray))
}

def getSQLTs(dt: Date): Timestamp = new Timestamp(dt.getTime)

def priceResultAsTuple(pr: PriceResult) = 
	(pr.max_ask, getSQLTs(pr.max_ask_dt), pr.close_ask, getSQLTs(pr.close_ask_dt), 
		pr.max_bid, getSQLTs(pr.max_bid_dt), pr.close_bid, getSQLTs(pr.close_bid_dt))


def reduceGroupedTickNfpData(xs: Array[Int], 
	itr : Iterable[Iterable[TickNfpData]]) : Iterable[Row] = {

	itr map aggregateTickGroup(xs, _)
}
//process all the groiup of TickNfpData to a single PriceReult for each timeframe
val processedPriceResults = tickDayCurrPairGroupRDD mapPartitions(reduceGroupedTickNfpData(timeframes , _))
//generate the new schema for basic data and all time frame data
val newSchema = newStructTypeForAllTimeframes(timeframes, nfp_hist_schema)

val newDataFrame = hiveContext.createDataFrame(processedPriceResults, newSchema)

tickDayCurrPairGroupRDD.unpersist