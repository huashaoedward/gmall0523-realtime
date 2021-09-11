package com.huashao.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.huashao.gmall.realtime.bean.SpuInfo
import com.huashao.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Author: huashao
  * Desc: 读取商品Spu维度数据到Hbase
  */
object SpuInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SpuInfoApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_spu_info";
    val groupId = "dim_spu_info_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform {
      rdd =>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //转换结构
    val objectDstream: DStream[SpuInfo] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: SpuInfo = JSON.parseObject(jsonStr, classOf[SpuInfo])
        obj
      }
    }

    //保存到Hbase
    import org.apache.phoenix.spark._
    objectDstream.foreachRDD{
      rdd=>
      rdd.saveToPhoenix(
        "GMALL0523_SPU_INFO",
      //列名
        Seq("ID", "SPU_NAME"),
        //hadoop的配置文件
        new Configuration,
        //zk的url
        Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}

