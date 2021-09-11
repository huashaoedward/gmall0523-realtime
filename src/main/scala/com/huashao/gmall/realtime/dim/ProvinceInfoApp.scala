package com.huashao.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.huashao.gmall.realtime.bean.ProvinceInfo
import com.huashao.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: huashao
  * Date: 2021/8/30
  * Desc:  从Kafka中读取省份数据，保存到Phoenix
 * maxwell监控到mysql中省份维度表有数据变化，会将数据写到kafka中；然后本程序从这个kafka读取数据，并保存到Phoenix中
  */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    //采集周期5s
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"

    //==============1.从Kafka中读取数据===============
    //1.1获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    //1.2根据偏移量获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size >0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //1.3获取当前批次获取偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        /*
        因为recodeDStream底层封装的是KafkaRDD，KafkaRDD混入了HasOffsetRanges
        特质，这个特质中提供了可以获取偏移量范围的方法
        先让KafkaRDD强转换成父类HasOffsetRanges，再取属性offsetRanges；有继承才能强转；
        offsetRanges是OffsetRange的数组，OffsetRange有几个属性，topic,partition,开始和结束offset
        */
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //2.===========保存数据到Phoenix===========
    //注意导包，用于隐式转换
    import org.apache.phoenix.spark._

    offsetDStream.foreachRDD{
      rdd=>{
        //先将rdd中的数据类型从ConsumerRecord，转为ProvinceInfo
        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            //获取省份的json格式字符串
            val jsonStr: String = record.value()
            //将json格式字符串封装为ProvinceInfo对象
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            provinceInfo
          }
        }
        //注意这个方法的rdd调用的，不是流
        provinceInfoRDD.saveToPhoenix(
          "GMALL0523_PROVINCE_INFO",
          //用seq来存入列名
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          //hadoop的配置文件 conf
          new Configuration,
          //zk的url
          Some("hadoop202,hadoop203,hadoop204:2181")
        )

        //保存偏移量到redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
