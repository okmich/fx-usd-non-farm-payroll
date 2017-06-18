import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

import java.math.{BigDecimal => JBigDecimal}


val hiveContext = new HiveContext(sc)

val nfp_hist = hiveContext.read.table("fx_nfp.nfp_hist").select("trading_day", "trading_hour", "trading_min", "previoius", "forecast", "actual").map((row: Row) => {
	(row.getString(0), row.getString(1).toInt, row.getString(2).toInt, row.getInt(3), row.getInt(4), row.getInt(5))
}).cache
val nfp_hist_map = (nfp_hist map (h => (h._1, h))).collect.toMap

val broad_nfp_hist = sc.broadcast(nfp_hist_map)

val tick_data_df = hiveContext.read.table("fx_nfp.tick_data").select("day","hour","min","sec","milli","bid","ask","curr_pair")

case class TickData(val day: String, val hour: Int, val min: Int, val sec: Int, val bid: JBigDecimal, val ask:JBigDecimal, val currPair: String) extends java.io.Serializable

case class TickNfpData(val day: String, val hour: Int, val min: Int, val sec: Int, val milli: Int, val bid: JBigDecimal, val ask:JBigDecimal, val currPair: String, val nfpday: String, val nfpHour: Int, val nfpMin: Int, val previous: Int, val forecast: Int, val actual: Int) extends java.io.Serializable

val tick_data_rdd = tick_data_df.map((r: Row) => (r.getString(0),r.getString(1).toInt,r.getString(2).toInt,r.getString(3).toInt,r.getString(4).toInt,r.getAs[JBigDecimal](5),r.getAs[JBigDecimal](6),r.getString(7)))

val tickNfpDataRdd = tick_data_rdd  mapPartitions (itr => {
	val nfpHistMap = broad_nfp_hist.value
	itr map (tp => {
		 nfpHistMap.get(tp._1) match {
		 	case None => null
		 	case Some(nfp) => TickNfpData(tp._1, tp._2, tp._3, tp._4, tp._5, tp._6, tp._7, tp._8, nfp._1, nfp._2, nfp._3, nfp._4, nfp._5, nfp._6)
		 }
	}) filter (_ != null) map (t => (t.day + t.currPair, t))
}) groupByKey
 
val tickDayCurrPairGroupRDD = tickNfpDataRdd.cache

import org.joda.time.LocalDateTime

def priceDtFromTickData(tick : TickNfpData) : LocalDateTime = {
	new LocalDateTime(
		tick.day.substring(0,4).toInt,
		tick.day.substring(4,6).toInt,
		tick.day.substring(6,8).toInt,
		tick.hour,
		tick.min,
		tick.sec,
		tick.milli
	)
}

def reportDtFromTickData(tick : TickNfpData) : LocalDateTime = {
	new LocalDateTime(
		tick.nfpday.substring(0,4).toInt,
		tick.nfpday.substring(4,6).toInt,
		tick.nfpday.substring(6,8).toInt,
		tick.nfpHour,
		tick.nfpMin,
		0,
		0
	)
}

val time = 5

def isWithinXmins(tick: TickNfpData, x: Int) : Boolean = {
	val tradingDt = priceDtFromTickData(tick)
	val reportDt = reportDtFromTickData(tick)
	val afterReport = tradingDt.compareTo(reportDt) > 0
	val beforeReportPlus = tradingDt.compareTo(reportDt.plusMinutes(x)) <= 0

	afterReport && beforeReportPlus
}
val max = tickPrices.filter(isWithinXmins(_, 5)).map(t => (t.ask, t.bid)).reduce((a,b) => (a._1.max(b._1), a._2.max(b._2)))


def seqOp (pr : PriceResult, tick: TickNfpData): PriceResult = {
	
}

def combineOp (pr1 : PriceResult, pr2 : PriceResult): PriceResult = {
	
}

val aggregate = tickPrices.filter(isWithinXmins(_, 5)).aggregate(PriceResult.init)(seqOp, combineOp)