package com.huashao.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
  * Author: huashao
  * Date: 2021/8/27
  * Desc: 维护偏移量的工具类：从redis获取offset，和往redis保存offset
 *
 * 使用的是redis类型中的hash， 批量读取hgetAll(offsetKey); 批量写入hmset(offsetKey,offsetMap)
 * 为什么选择hash类型？因为offset是map类型，Map[TopicPartition,Long] ,可以存入map?
  */
object OffsetManagerUtil {

  /*
  从Redis中获取偏移量
  注意返回值是Map[TopicPartition,Long]，而且是scala类型的map。
  type:hash   key: offset:topic:groupId   field:partition   value: 偏移量
  注意这里的offset是使用的是kafka的固定的类型，是Map[TopicPartition,Long]；
  而且MyKafkaUtil中的根据偏移量从kafka读取数据的方法参数，是封装好的，就是这个offset，是固定格式。
  */
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long]={
    //获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key     offset:topic:groupId
    var offsetKey = "offset:" + topic + ":" + groupId

    //获取当前消费者组消费的主题  对应的分区以及偏移量；
    // hgetAll是获取Hash中同一个key的所有数据，但它默认返回java的map，要转换为scala的map
    //因为同一个key，即同一个主题的同一消费组，会有多个不同partition对应的偏移量，所以用map封装
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    /*因为redis是java写的，所以查询返回就是java的map；Kafka是scala写的，所以要转换，
    将java的map转换为scala的map,因为scala的集合好操作，
    map是映射，返回是二元组，再把二元组(TopicPartition对象，offset)，
    转成scala的Map[TopicPartition,Long]格式的map
     */
    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        //Map[TopicPartition,Long]
        //创建TopicPartition对象，要传入topic和partition
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    //返回oMap
    oMap
  }


  //将偏移量信息保存到Redis中，不需要返回值
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //拼接redis中操作偏移量的key
    var offsetKey = "offset:" + topic + ":" + groupId
    //定义java的map集合，用于存放每个分区对应的偏移量。用map是因为后面的jedis.hmset()要传入map批量写入
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
    //对offsetRanges进行遍历，将数据封装offsetMap
    //offsetRanges是一个数组，里面是同一个topic的不同partition对应的offset
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset

      //offsetMap里都是String类型，因为要存入redis中
      offsetMap.put(partitionId.toString,untilOffset.toString)

      println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    }

    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //多个写入hash中，hmset,要传入 标识hash的key，和对应的map（分区和结束偏移量）
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }

}
