
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._


val hiveContext = new HiveContext(sc)

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



:load TimeFrameAggregator.scala
val tfAgg = new TimeFrameAggregator

val oneMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(1)).as("omb")).
select(concat($"curr_pair", $"day").as("key"), $"curr_pair", $"day", $"trading_hour", $"trading_min", $"omb.open_price", $"omb.open_price_ts", $"omb.close_price", $"omb.close_price_ts")

val fiveMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(5)).as("omb")).
select(concat($"curr_pair", $"day").as("key"), $"curr_pair", $"day", $"trading_hour", $"trading_min", $"omb.close_price", $"omb.close_price_ts")

val fifteenMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(15)).as("omb")).
select(concat($"curr_pair", $"day").as("key"), $"curr_pair", $"day", $"trading_hour", $"trading_min", $"omb.close_price", $"omb.close_price_ts")


val numSeq = Seq(1,5,10,20)
val seqDFs = numSeq.map(n => {
		aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(n)).as("omb")).
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


aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(60)).as("omb")).
select("curr_pair", "day", "trading_hour", "trading_min","omb.close_price","omb.close_price_ts").show


