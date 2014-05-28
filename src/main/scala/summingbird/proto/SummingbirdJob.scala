package summingbird.proto

import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher

case class OrderViewed (
  val productId: Long,
  val requestTime: java.util.Date,
  val orderjson: String
)

object ViewCount {
  /**
    * These two items are required to run Summingbird in
    * batch/realtime mode, across the boundary between storm and
    * scalding jobs.
    */
  implicit val timeOf: TimeExtractor[OrderViewed] = TimeExtractor(_.requestTime.getTime)
  //implicit val batcher = Batcher.ofHours(1)
  implicit val batcher = Batcher.ofMinutes(5)

  /**
    * The actual Summingbird job. Notice that the execution platform
    * "P" stays abstract. This job will work just as well in memory,
    * in Storm or in Scalding, or in any future platform supported by
    * Summingbird.
    */
  def viewCount[P <: Platform[P]](
    source: Producer[P, OrderViewed],
    store: P#Store[Long, Long]) =
    source
      .flatMap { event: OrderViewed => Seq((event.productId -> 1L)) }
      .sumByKey(store)
}
