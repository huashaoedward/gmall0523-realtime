package com.huashao.gmall.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * Author: huashao
  * Date: 2021/9/6
  * Desc: 从MySQL中读取kafka的偏移量的工具类，底层调用MySQLUtil的方法
  */
object OffsetManagerM {

  //获取偏移量
  /**
    * 从Mysql中读取Kafka偏移量
    * @param consumerGroupId  消费者组
    * @param topic  主题
    * @return   返回值还是Map[TopicPartition, Long]
    */
  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {

    val sql=" select group_id,topic,topic_offset,partition_id from offset_0523 " +
      " where topic='"+topic+"' and group_id='"+consumerGroupId+"'"

    //查询mysql后得到的结果是List,里面是JSONObject
    val jsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    /*
    对List里的每一条JSONObject进行映射并返回二元组(topicPartition, offset)，
    结构转换：  List[JSONObject] => List[(TopicPartition, Long)]
     */
    val topicPartitionList: List[(TopicPartition, Long)] = jsonObjList.map {
      jsonObj =>{
        //创建 TopicPartition对象，要传入是的topic和partitionId
        val topicPartition: TopicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
        //偏移量
        val offset: Long = jsonObj.getLongValue("topic_offset")
        //返回二元组，最终返回值是List里面是一个个二元组
        (topicPartition, offset)
      }
    }
    //List[(TopicPartition, Long)]的结构，再转为Map[TopicPartition, Long]
    val topicPartitionMap: Map[TopicPartition, Long] = topicPartitionList.toMap
    topicPartitionMap
  }

}
