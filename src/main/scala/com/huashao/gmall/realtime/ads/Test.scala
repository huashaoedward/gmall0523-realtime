package com.huashao.gmall.realtime.ads

import com.huashao.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Author: Felix
  * Date: 2020/11/3
  * Desc: 
  */
object Test {
  var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "test"
    var groupId = "test_group"

    val tuple: (DStream[ConsumerRecord[String, String]], Array[OffsetRange]) = get(ssc,topic,groupId)
    val offsetRanges: Array[OffsetRange] = tuple._2

    val recordDS: DStream[ConsumerRecord[String, String]] = tuple._1
    val valueDS: DStream[String] = recordDS.map(_.value())
    valueDS.print(100)
    valueDS.cache()
    //println(offsetRanges.size)
    valueDS.foreachRDD{
      rdd=>{
        println(offsetRanges.mkString("\n"))
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
  def get(ssc:StreamingContext,topic:String,groupId: String): Tuple2[DStream[ConsumerRecord[String, String]],Array[OffsetRange]] ={

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
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => {
        //每个采集周期执行一次，周期性在driver中执行
        offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    (inputGetOffsetDstream,offsetRanges)
  }
}
