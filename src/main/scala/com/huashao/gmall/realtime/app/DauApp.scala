package com.huashao.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.huashao.gmall.realtime.bean.DauInfo
import com.huashao.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
/**
  * Author: huashao
  * Date: 2021/8/25
  * Desc:  日活业务：统计的是同一天内，活跃的设备数
 *
 * 要点：
 * 以分区为单位建立redis连接，
 * kafka RDD的offsetRange的获取，
  */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    //采集周期是5秒，微批处理
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    //从启动日志读取数据
    var topic:String = "gmall_start_0523"
    var groupId:String = "gmall_dau_0523"

    //从Redis中获取Kafka分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    //Spark从Kafka中读取的数据流，流中的类型是ConsumerRecord
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照默认配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值,
    //这里先声明一个空的数组，泛型是OffsetRange，是不null
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    /*
    使用transform，不做任何操作，只拿offset，
    */
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        /*因为recodeDStream底层封装的是KafkaRDD，KafkaRDD混入了HasOffsetRanges
        特质，这个特质中提供了可以获取偏移量范围的方法
        先让KafkaRDD强转换成父类HasOffsetRanges，再取属性offsetRanges；有继承才能强转；
        offsetRanges是OffsetRange的数组，OffsetRange有几个属性，topic,partition,开始和结束offset
        */
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //不做任何操作，还是返回rdd
        rdd
      }
    }

    //将流中的类型从ConsumerRecord转成JSONObject,并补充dt和hr字段的数据
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //Kafka中的消息是kv，k用于分区的，v才是消息体
        val jsonString: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //从json对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转换为日期和小时  2020-10-21 16
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        //获取天 2020-10-21
        var dt = dateStrArr(0)
        //获取小时 16
        var hr = dateStrArr(1)
        //将天和小时，存入jsonObject中
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    //jsonObjDStream.print(1000)


    /*
    //通过Redis   对采集到的启动日志进行去重操作  方案1  采集周期中的每条数据都要获取一次Redis的连接，连接过于频繁
    //redis 类型 set    key：  dau：2020-10-23    value: mid    expire   3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
      jsonObj => {
        //获取登录日期
        val dt = jsonObj.getString("dt")
        //获取设备id
        val mid = jsonObj.getJSONObject("common").getString("mid")
        //拼接Redis中保存登录信息的key
        var dauKey = "dau:" + dt
        //获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //从redis中判断当前设置是否已经登录过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        //设置key的失效时间，要加判断，避免对同一天内多个相同key的数据一直设24小时，那会一直推长下去。没设是-1；
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        //关闭连接
        jedis.close()

        if (isFirst == 1L) {
          //说明是第一次登录
          true
        } else {
          //说明今天已经登录过了
          false
        }
      }
    }
    */
    //通过Redis   对采集到的启动日志进行去重操作
    // 方案2  以分区为单位对数据进行处理，每一个分区获取一次Redis的连接，所以使用mapPartitions
    //redis 类型 set    key：  dau：2020-10-23    value: mid    expire   3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => { //以分区为单位对数据进行处理
        //每一个分区获取一次Redis的连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个ListBuffer，泛型是JSONObject，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        //对分区的所有数据进行遍历
        for (jsonObj <- jsonObjItr) {
          //获取日期
          val dt = jsonObj.getString("dt")
          //获取设备id
          val mid = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作redis的key
          var dauKey = "dau:" + dt
          //在redis中，将同一天的mid放入set中，会自动去重，用于统计日活
          val isFirst = jedis.sadd(dauKey, mid)

          //设置key的失效时间,要加判断，避免对同一天内多个相同key的数据一直设24小时，那会一直推长下去。没设默认是-1；
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //说明是当天的第一次登录，第一次登录的日志数据，单独放入ListBuffer。是用于？
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        //把ListBuffer转为可迭代的。每天的每一条首次登录的start日志，会被放入ListBuffer，转换并返回流中，格式还是JSONObject
        filteredList.toIterator
      }
    }

    //filteredDStream.count().print()

    /*
    将数据批量的保存到ES中，
    先将流中的JSONObject转为二元组(mid,DauInfo)，所有的二元组组成List,将List传给ES工具类并批量插入ES中
    最后提交偏移量到Redis中。
     */
    filteredDStream.foreachRDD{
      rdd=>{
        //以分区为单位对数据进行处理，所以用foreachPartition
        rdd.foreachPartition{
          jsonObjItr=>{
            /*
            dauInfoList存放的是二元组，(mid,DauInfo),mid作为存入es的文档id,
            在put方法中传入，保证了put的幂等性。如果不传，系统只会生成一个id
             */
            val dauInfoList: List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //jsonObjItr是一个分区中所有的jsonObj组成的可迭代
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                //创建DauInfo对象
                val dauInfo = DauInfo(
                  //从json对象里取String字段
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  //从根json对象里取String字段
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                //转化后返回二元组，(mid,dauInfo)，mid作为存入es的文档id，保证幂等性
                (dauInfo.mid,dauInfo)
              }
                //再转成集合List[(String,DauInfo)]，是一个分区partition里的批量数据，一个个二元组放入List
            }.toList

            //将数据批量的保存到ES中
            //获取当前日期，并转化成"yyyy-MM-dd"的格式的string
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            //批量插入ES中
            MyESUtil.bulkInsert(dauInfoList,"gmall0523_dau_info_" + dt)
          }
        }
        //提交偏移量到Redis中，offsetRanges在之前的转换中已获取到
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}