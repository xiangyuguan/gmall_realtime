package App

import java.text.SimpleDateFormat
import java.util.Date
import Util.{KafkaUtil, RedisUtil}
import bean.StartupLog
import com.alibaba.fastjson.JSON
import com.wuhui.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

//日活
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dauapp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //实时读数据从Kafka
    val sourceStream: DStream[String] = KafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)

    val objStream: DStream[StartupLog] = {
      sourceStream.map((jsonStr: String) => JSON.parseObject(jsonStr, classOf[StartupLog]))
    }

    // 2.2 写入之前先做过滤
    var filteredStartupLogDStream: DStream[StartupLog] = objStream.transform((rdd: RDD[StartupLog]) => {

      val client = RedisUtil.getJedisClient
      val midSet = client.smembers(Constant.REDIS_DAU_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      val midSetBC = ssc.sparkContext.broadcast(midSet)
      client.close()
      rdd.filter(startupLog => {
        val mids = midSetBC.value
        // 返回没有写过的
        !mids.contains(startupLog.mid)
      })
    })


    // 2.3 批次内去重:  如果一个批次内, 一个设备多次启动(对这个设备来说是第一个批次), 则前面的没有完成去重
    filteredStartupLogDStream = filteredStartupLogDStream
      .map((log: StartupLog) => (log.mid, log))
      .groupByKey
      .flatMap {
        case (_, logIt) => logIt.toList.sortBy(_.ts).take(1)
      }


    // 2.4 写入到redis
    filteredStartupLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(startupLogIt => {
        val client = RedisUtil.getJedisClient
        val startupLogList = startupLogIt.toList
        startupLogList.foreach(startupLog => {
          // 写入到redis的set中
          client.sadd(Constant.REDIS_DAU_KEY + ":" + startupLog.logDate, startupLog.mid)
        })
        client.close()
      })
    })


    // 2.5 写入到 Phoenix(HBase)
    import org.apache.phoenix.spark._
    filteredStartupLogDStream.foreachRDD(rdd => {
      rdd.foreach(log => {
        println(log.logType)
      })
      // 参数1: 表名  参数2: 列名组成的 seq 参数 zkUrl: zookeeper 地址
      rdd.saveToPhoenix(
        Constant.TABLE_DAU,
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("node103,node104,node105:2181"))
    })

    filteredStartupLogDStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
