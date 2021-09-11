package com.huashao.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.huashao.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil,
  OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: huashao
  * Date: 2021/8/29
  * Desc: 从Kafka中读取数据，根据表名进行分流处理（canal）
 *        在本程序之前，已经有canal对数据库中被监控的表的数据变化，记录到binlog，
 *        并被canal获取，以json方式发送到kafka的主题gmall0523_db_c中。
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "gmall0523_db_c"
    var groupId = "base_db_canal_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size > 0){
      //从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      //从最新的位置开始消费,默认的配置是latest
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前批次读取的Kafka主题中偏移量信息
    //这里先声明一个空的数组，泛型是OffsetRange，是不null
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        /*因为recodeDStream底层封装的是KafkaRDD，KafkaRDD混入了HasOffsetRanges特质，
        这个特质中提供了可以获取偏移量范围的方法
        先让KafkaRDD强转换成父类HasOffsetRanges，再取属性offsetRanges；有继承才能强转；
        offsetRanges是OffsetRange的数组，OffsetRange有几个属性，topic,partition,开始和结束offset
        */
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对接收到的数据进行结构的转换，ConsumerRecord[String,String(jsonStr)]====>jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取json格式的字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    /*
    分流：根据不同的表名，将数据发送到不同的kafka主题中去
    json数据中有table和type这些字段

    jsonObjDStream.foreachRDD是遍历流中的每一条RDD；
    rdd.foreach是分布式遍历RDD中的每一个元素，调用指定函数
     */
    jsonObjDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          jsonObj=>{
            //获取操作类型
            val opType: String = jsonObj.getString("type")
            /*
            只处理新增的插入操作？？因canal只能处理最近更新数据，不能处理历史数据，
            而且更新的数据的类型是insert；而maxwell则可处理历史数据，类型是bootstrap insert.
             */
            if("INSERT".equals(opType)){
              //获取表名
              val tableName: String = jsonObj.getString("table")
              //获取操作数据，
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接目标topic名称
              var sendTopic= "ods_" + tableName
              //对data数组进行遍历，先把java的数组转成scala的数组，因为getJSONArray()只能拿到java数组
              import scala.collection.JavaConverters._
              for (dataJson <- dataArr.asScala) {
                //根据表名将数据发送到不同的主题中去
                MyKafkaSink.send(sendTopic,dataJson.toString)
              }
            }
          }
        }
        //往redis提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
