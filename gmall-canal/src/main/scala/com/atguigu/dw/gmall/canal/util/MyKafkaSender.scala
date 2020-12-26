package com.atguigu.dw.gmall.canal.util


import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Author lzc
  * Date 2019/5/17 4:45 PM
  */
object MyKafkaSender {
  val props = new Properties()
  // Kafka服务端的主机名和端口号
  props.put("bootstrap.servers", "node103:9092,node104:9092,node105:9092")
  // key序列化
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // value序列化
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  def sendToKafka(topic: String, content: String) = {
    producer.send(new ProducerRecord[String, String](topic, content))
  }
}
