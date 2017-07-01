import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import udaf.TimeFrameAggregator

object App extends java.io.Serializable {

	def main(args: Array[String]) : Unit = {
		val opts = parseCmdLineArgs(args)

		val sparkConf = new SparkConf

		val sc = new SparkContext(sparkConf)
		val hiveContext =  new HiveContext(sc)
		import hiveContext.implicits._
		
		val ocAgg = new OpenCloseAggregator

		val nfp_hist_df = hiveContext.read.table("fx_nfp.nfp_hist").
			select("trading_day", "trading_hour", "trading_min", "previoius", "forecast", "actual")

		val tick_data_df = hiveContext.read.table("fx_nfp.tick_data").
			select("day","hour","min","sec","milli","bid","ask","curr_pair")

		val joinedDF = tick_data_df.join(nfp_hist_df, $"day" === $"trading_day")
		//cast all the columns 

		val txnformedDF = joinedDF.select($"day",$"hour".cast("int"),$"min".cast("int"),
			$"sec".cast("int"),$"milli".cast("int"),$"bid",$"ask",$"curr_pair",
			$"trading_day", $"trading_hour".cast("int"), $"trading_min".cast("int"), $"previoius".as("previous"), $"forecast", $"actual").cache

		val aggGroupedData = txnformedDF.groupBy($"curr_pair", $"day", $"trading_hour", $"trading_min")

		val numSeq = opts("timeFrames").split(",").map(_.toInt)
		val seqDFs = numSeq.map(n => {
				aggGroupedData.agg(ocAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(n)).as("omb")).
					select(concat($"curr_pair", $"day").as("key"+n), 
						$"omb.open_price".as("open_price_"+n), $"omb.open_price_ts".as("open_price_ts_"+n), 
						$"omb.close_price".as("close_price_"+n), $"omb.close_price_ts".as("close_price_ts_"+n))
			}).zip(numSeq)

		val DF = txnformedDF.select(concat($"curr_pair", $"day").as("key0"), 
			$"previous", $"forecast", $"actual", $"curr_pair", 
			$"day", concat($"trading_hour", $"trading_min").as("time")).distinct


		val aggdTable = seqDFs.reduce((f: (DataFrame, Int), g: (DataFrame, Int)) => {
				val xDF = f._1
				val yDF = g._1
				(xDF.join(yDF, xDF("key"+ f._2) === yDF("key"+g._2), "inner"), f._2)
			})._1

		val newKey = numSeq map ("key"+_) head

		val finalTable = DF.join(aggdTable, $"key0" === aggdTable(newKey), "inner")

		//coalesce output to a single file
		finalTable.coalesce(1).write.saveAsTable(s"${opts("dbName")}.${opts("table")}")
	}

	def parseCmdLineArgs(strs: Array[String]) : Map[String, String] = {
		strs.map(item => {
			val parts = item.split(":")
			(parts(0).substring(1) -> parts(1))
			}).toMap
	}
}

