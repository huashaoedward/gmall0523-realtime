package com.huashao.gmall.realtime.ads

import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/11/3
  * Desc: 
  */
object Test1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "test"
    var groupId = "test_group"

    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到Kafka中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点

    val recordDStream: DStream[ConsumerRecord[String, String]] =
      recordInputDstream.transform {
      rdd => {
        offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val valueDS: DStream[String] = recordDStream.map(_.value())

    valueDS.print(100)

    //println只会执行一次，但是foreachRDD中的代码会每批次都执行一次
    println(offsetRanges.size)
    //println(offsetRanges.mkString("****\n"))
   /*valueDS.foreachRDD {
     rdd => {
       println(offsetRanges.mkString("\n"))
     }
   }*/

    ssc.start()
    ssc.awaitTermination()
  }

}
