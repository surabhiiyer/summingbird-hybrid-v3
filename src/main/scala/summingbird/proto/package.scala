package summingbird

package object proto {
  import ViewCount._
  import com.twitter.summingbird.batch.BatchID
  import java.text.SimpleDateFormat
  import java.util.{Date, TimeZone}

  val random = new scala.util.Random

  val JobDir = "summingbird/tmp/summingbird-proto/"
  val DataDir = JobDir + "data/"

  val KafkaZkConnectionString = "stage-pf8.stage.ch.flipkart.com:2181/kafka/bigfoot/fireball_1"
  //val KafkaTopic = "summingbird.proto.productview"
  val KafkaTopic = "summingbird.orders"
  val MaxId = 10

  val DataFileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
  DataFileDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  def dataFileForBatch(batch: BatchID) = {
    // timestamp is the end time of the batch
    DataDir + "productview_0_" + DataFileDateFormat.format(batcher.earliestTimeOf(batch.next).toDate)
  }

  def randomView(date: Date = new Date()) = {
    ProductViewed(
      random.nextLong.abs % MaxId,
      date,
      " {\"msgId\":\"1a3060d3-8ccc-4578-8c22-f2c8443a6887\",\"body\":\"{\\\"bill_from_party_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"bill_to_party_id\\\":\\\"ACC1374529230395206\\\",\\\"billing_address_id\\\":\\\"ADD13745292306179109\\\",\\\"billing_amount\\\":2621.0,\\\"created_at\\\":\\\"2013-07-23T03:10:31+05:30\\\",\\\"created_by\\\":\\\"website\\\",\\\"currency\\\":\\\"INR\\\",\\\"external_id\\\":\\\"OD323070360000\\\",\\\"gift_wrap\\\":false,\\\"guest\\\":false,\\\"id\\\":33137,\\\"order_date\\\":\\\"2013-07-23T03:10:30+05:30\\\",\\\"original_billing_amount\\\":2631.0,\\\"sales_channel\\\":\\\"WEB\\\",\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"shipping_address_id\\\":\\\"ADD13745292306179109\\\",\\\"status\\\":\\\"approved\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"order_items\\\":[{\\\"category_id\\\":20056,\\\"created_at\\\":\\\"2013-07-23T03:10:31+05:30\\\",\\\"deliver_before_date\\\":\\\"2013-07-27T03:10:30+05:30\\\",\\\"id\\\":3106659,\\\"internal_sla\\\":4,\\\"is_computer\\\":false,\\\"list_price\\\":2621,\\\"listing_id\\\":\\\"LSTREMD7KK6UV72QFTGXQF1NU\\\",\\\"order_date\\\":\\\"2013-07-23T03:10:30+05:30\\\",\\\"order_id\\\":33137,\\\"order_item_type_id\\\":\\\"physical\\\",\\\"product_id\\\":\\\"REMD7KK6UV72QFTG\\\",\\\"quantity\\\":1,\\\"sale_package\\\":\\\"{\\\\\\\"sales_package\\\\\\\":\\\\\\\"Remote Controller::Battery::Manual\\\\\\\",\\\\\\\"hidden_sales_package\\\\\\\":null}\\\",\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"selling_price\\\":2621,\\\"sku\\\":\\\"SKU0000000000000\\\",\\\"status\\\":\\\"approved\\\",\\\"sub_status\\\":\\\"payment_approved\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"title\\\":\\\"Some Mobile\\\",\\\"order_item_adjustments\\\":[],\\\"order_item_units\\\":[{\\\"created_at\\\":\\\"2013-07-23T03:10:31+05:30\\\",\\\"id\\\":46249,\\\"order_item_id\\\":1106658,\\\"promised_date\\\":\\\"2013-07-27T03:10:30+05:30\\\",\\\"quantity\\\":1,\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"sla\\\":4,\\\"status\\\":\\\"approved\\\",\\\"sub_status\\\":\\\"payment_approved\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"warehouse\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"order_item_unit_status_histories\\\":[{\\\"change_data\\\":null,\\\"change_reason\\\":null,\\\"change_sub_reason\\\":null,\\\"created_at\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"event\\\":\\\"approve\\\",\\\"from_status\\\":\\\"created\\\",\\\"from_sub_status\\\":null,\\\"id\\\":71822,\\\"order_item_id\\\":1106658,\\\"order_item_unit_id\\\":46249,\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"status_time\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"to_status\\\":\\\"approved\\\",\\\"to_sub_status\\\":\\\"payment_approved\\\",\\\"user_login\\\":\\\"sc_automation\\\"}]}],\\\"super_category\\\":\\\"BMV\\\",\\\"category\\\":\\\"TVVideo\\\",\\\"sub_category\\\":\\\"TVVideoAccessory\\\",\\\"vertical\\\":\\\"RemoteControl\\\",\\\"dispatch_after_date\\\":null,\\\"order_item_status_histories\\\":[{\\\"change_data\\\":null,\\\"change_reason\\\":null,\\\"change_sub_reason\\\":null,\\\"created_at\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"event\\\":\\\"approve\\\",\\\"from_status\\\":\\\"created\\\",\\\"from_sub_status\\\":null,\\\"id\\\":70464,\\\"order_id\\\":33137,\\\"order_item_id\\\":1106658,\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"status_time\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"to_status\\\":\\\"approved\\\",\\\"to_sub_status\\\":\\\"payment_approved\\\",\\\"user_login\\\":\\\"sc_automation\\\"}]}],\\\"order_adjustments\\\":[],\\\"order_channel\\\":{\\\"created_at\\\":\\\"2013-07-23T03:10:31+05:30\\\",\\\"id\\\":24281,\\\"order_id\\\":33137,\\\"sales_channel\\\":\\\"WEB\\\",\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"tenant_id\\\":\\\"FKMP\\\"},\\\"order_status_histories\\\":[{\\\"change_reason\\\":null,\\\"created_at\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"event\\\":\\\"approve\\\",\\\"from_status\\\":\\\"created\\\",\\\"id\\\":51884,\\\"order_id\\\":33137,\\\"seller_id\\\":\\\"v5lxo7hj3i7v4gre\\\",\\\"status_time\\\":\\\"2013-07-23T03:10:49+05:30\\\",\\\"tenant_id\\\":\\\"FKMP\\\",\\\"to_status\\\":\\\"approved\\\",\\\"user_login\\\":\\\"sc_automation\\\"}]}\",\"streamId\":\"orders\",\"headers\":{\"event_date\":\"2014-04-12T19:40:20+05:30\",\"parent_msg_id\":\"orders:2944:1374529251737\",\"business_entity\":\"oms.b2c.orders\"},\"receivedTime\":1374556715813} "
    )
  }

  def parseView(bytes: Array[Byte]): ProductViewed = {
    parseView(new String(bytes))
  }

  def parseView(s: String): ProductViewed = {
    val bits = s.split("\t")
    ProductViewed(bits(0).toLong, new Date(bits(1).toLong), bits(2))
  }

  def serializeView(pdpView: ProductViewed): String = {
    "%s\t%s\t%s".format(pdpView.productId, pdpView.requestTime.getTime, pdpView.userGuid)
  }

}
