package Util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaUtil {

  private val params = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProp("kafka.servers"),
    ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProp("kafka.group.id"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  //从kafka读数据
  def getKafkaStream(ssc: StreamingContext, topic: String, otherTopic: String*) = {
    val topics = Set(topic) ++ otherTopic
    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, params)
    ).map(_.value())
  }
}
