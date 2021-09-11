package com.huashao.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.huashao.gmall.realtime.bean.UserInfo
import com.huashao.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, streaming}

/**
  * Author: huashao
  * Date: 2021/9/1
  * Desc: 从Kafka中读取数据，保存到Phoenix
 * maxwell监控到mysql中用户维度表有数据变化，会将数据写到kafka中的ods_user_info主题；
 * 然后本程序从这个kafka读取数据，并保存到Phoenix中
  */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.1基本环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("userInfoApp")
    val ssc = new StreamingContext(conf, streaming.Seconds(5))
    val topic = "ods_user_info"
    val groupId = "user_info_group"

    //1.2获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var offsetDStream: InputDStream[ConsumerRecord[String, String]] = null

    //为什么要判断不为Null后，还要判断长度>0？
    //因为getString()获取数据时，没数据，那就会将数据置为空的字符串，而不是Null；空的字符串，本身是有对象的。
    if (offsetMap != null && offsetMap.size != 0) {
      offsetDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      offsetDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //1.3获取本批次消费数据的偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetRangeDSteram: DStream[ConsumerRecord[String, String]] = offsetDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //1.4对从Kafka中读取的数据进行结构的转换  record(kv)==>UserInfo
    //map是对调用者offsetRangeDSteram内部的数据映射
    val userInfoDStream: DStream[UserInfo] = offsetRangeDSteram.map {
      record => {
        val jsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])

        //把生日转成年龄，根据年龄给userInfo的年龄段赋值
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val  betweenMs = curTs - date.getTime
        val age = betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20岁及以下"
        }else if(age>30){
          userInfo.age_group="30岁以上"
        }else{
          userInfo.age_group="21岁到30岁"
        }

        //userInfo的性别备注
        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }
        userInfo
      }
    }

    //1.5保存到Phoenix中
    userInfoDStream.foreachRDD{
      rdd=>{
        //隐式转换
        import org.apache.phoenix.spark._
        //流中的RDD，直接存入Phoenix中
        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
        rdd.saveToPhoenix(
          "GMALL0523_USER_INFO",
          //用seq来存入列名
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          //hadoop的配置文件
          new Configuration,
          //zk的url
          Some("hadoop102,hadoop103,hadoop104:2181")
        )

        //1.6提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
