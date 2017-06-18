import java.math.{BigDecimal => JBigDecimal}
import java.util.Date

class PriceResult(val max_ask: JBigDecimal, val max_ask_dt: Date, 
				val close_ask: JBigDecimal, val close_ask_dt: Date, 
				val max_bid: JBigDecimal, val max_bid_dt: Date, 
				val close_bid: JBigDecimal, val close_bid_dt: Date) extends java.io.Serializable {

	override def toString : String = s"PriceResult($max_ask, $max_ask_dt, $close_ask, $close_ask_dt, $max_bid, $max_bid_dt, $close_bid, $close_bid_dt)"
}

object PriceResult {
	private val ZERO = JBigDecimal.ZERO

	private val ZERO_DT = new Date(0)

	def apply(max_ask: JBigDecimal, max_ask_dt: Date,
				close_ask: JBigDecimal, close_ask_dt: Date,
				max_bid: JBigDecimal, max_bid_dt: Date,
				close_bid: JBigDecimal, close_bid_dt: Date) : PriceResult ={
		new PriceResult(max_ask, max_ask_dt, close_ask, close_ask_dt, max_bid, max_bid_dt, close_bid, close_bid_dt)
	}

	def init : PriceResult = {
		new PriceResult(ZERO, ZERO_DT, ZERO, ZERO_DT, ZERO, ZERO_DT, ZERO, ZERO_DT)
	}
}