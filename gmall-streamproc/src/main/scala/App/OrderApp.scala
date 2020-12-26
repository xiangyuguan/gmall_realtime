package App

import java.text.SimpleDateFormat
import Util.KafkaUtil
import bean.OrderInfo
import com.alibaba.fastjson.JSON
import com.wuhui.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/5/17 5:31 PM
  */
object OrderApp {
  def main(args: Array[String]): Unit = {

    //1 . 从kafka消费数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(2))
    val sourceDStream = KafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER)
    val orerInfoDStream: DStream[OrderInfo] = sourceDStream.map { // 对数据格式做调整
      case (value) => {
        val orderInfo = JSON.parseObject(value, classOf[OrderInfo]) // 李小名 => 李**
        orderInfo.consignee = orderInfo.consignee.substring(0, 1) + "**" // 李小名 => 李**
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) +
          "****" + orderInfo.consignee_tel.substring(7, 11)

        // 计算 createDate 和 createHour
        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(orderInfo.create_time)
        orderInfo.create_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
        orderInfo.create_hour = new SimpleDateFormat("HH").format(date)
        orderInfo
      }
    }

    //2. 把数据写入到 Phoenix
    import org.apache.phoenix.spark._
    orerInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL_REALTIME.ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("node103,node104,node105:2181"))
    })
    orerInfoDStream.print
    ssc.start()
    ssc.awaitTermination()

  }
}
