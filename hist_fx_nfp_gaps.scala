import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

def readTradeGap(path:String,curr: String) : RDD[(String, Int, Long, Long)] = {
	val REGEXP = """Gap of ([0-9]+)s found between ([0-9]+) and ([0-9]+).""".r
	val parser = (s:String) => s match {
		case REGEXP(gap, starttime, endtime) => (curr, gap.toInt, starttime.toLong, endtime.toLong)
		case _ => (null, 0,0l,0l)
		}

	val rdd = sc.textFile(path)  map parser filter (_._2 != 0)
	
	rdd
}

val eurusd_gap_rdd = readTradeGap("/user/cloudera/rawdata/hist_fx_nfp/gaps/eurusd", "EURUSD")
val gbpusd_gap_rdd = readTradeGap("/user/cloudera/rawdata/hist_fx_nfp/gaps/gbpusd", "GBPUSD")
val usdjpy_gap_rdd = readTradeGap("/user/cloudera/rawdata/hist_fx_nfp/gaps/usdjpy", "USDJPY")

val gap_rdd = eurusd_gap_rdd.union(gbpusd_gap_rdd).union(usdjpy_gap_rdd)

val gap_df = gap_rdd.toDF("curr_pair", "gap", "startdt", "enddt").
	withColumn("day", substring($"startdt", 1, 8)).
	withColumn("start_hour", substring($"startdt", 9, 2)).
	withColumn("start_min", substring($"startdt", 11, 2)).
	withColumn("start_sec", substring($"startdt", 13, 2)).
	withColumn("end_hour", substring($"enddt", 9, 2)).
	withColumn("end_min", substring($"enddt", 11, 2)).
	withColumn("end_sec", substring($"enddt", 13, 2)).
	drop("startdt").
	drop("enddt").cache
