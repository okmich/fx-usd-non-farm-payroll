import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import java.math.{BigDecimal => JBigDecimal}

import org.apache.spark.sql.functions._

:load TimeFrameAggregator.scala

val hiveContext = new HiveContext(sc)

val tfAgg = new TimeFrameAggregator
hiveContext.udf.register("tfAgg", tfAgg)
	

val nfp_hist_df = hiveContext.read.table("fx_nfp.nfp_hist").
	select("trading_day", "trading_hour", "trading_min", "previoius", "forecast", "actual")

val tick_data_df = hiveContext.read.table("fx_nfp.tick_data").
	select("day","hour","min","sec","milli","bid","ask","curr_pair")

val joinedDF = tick_data_df.join(nfp_hist_df, $"day" === $"trading_day")
//cast all the columns 

val txnformedDF = joinedDF.select($"day",$"hour".cast("int"),$"min".cast("int"),
	$"sec".cast("int"),$"milli".cast("int"),$"bid",$"ask",$"curr_pair",
	$"trading_day", $"trading_hour".cast("int"), $"trading_min".cast("int"), $"previoius", $"forecast", $"actual").cache



val aggGroupedData = txnformedDF.groupBy($"curr_pair", $"day", $"trading_hour", $"trading_min")

:load TimeFrameAggregator.scala
val tfAgg = new TimeFrameAggregator
val oneMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(1)).as("omb"))

oneMinDF.select("omb.max_price","omb.max_price_ts","omb.close_price","omb.close_price_ts").show


val fiveMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(5)))

val fifteenMinDF = aggGroupedData.agg(tfAgg($"day",$"hour",$"min",$"sec",$"milli",$"trading_hour",$"trading_min",$"ask", lit(15)))




