package com.huashao.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.huashao.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo,
  UserStatus}
import com.huashao.gmall.realtime.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: huashao
  * Date: 2021/8/30
  * Desc:  首单分析，从Kafka中读取订单数据，并对其进行处理
 *
 * 要点：
 * 使用transform，以采集周期为单位，关联省份和用户维度的数据，
 * 广播变量；
 * RDD的缓存， rdd.cache()
 * saveToPhoenix这个方法的调用，可以把RDD直接保存入Hbase中
 *
  */
object OrderInfoApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    //采集周期5秒
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"

    //===================1.从Kafka主题中读取数据====================
    //从Redis获取历史偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    //根据偏移量是否存在决定从什么位置开始读取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null

    //为什么要判断不为Null后，还要判断长度>0？
    //因为getString()获取数据时，没数据，那就会将数据置为空的字符串，而不是Null；空的字符串，本身是有对象的。
    if(offsetMap!=null && offsetMap.size >0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前批次处理的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    /*
       因为recodeDStream底层封装的是KafkaRDD，KafkaRDD混入了HasOffsetRanges
       特质，这个特质中提供了可以获取偏移量范围的方法
       先让KafkaRDD强转换成父类HasOffsetRanges，再取属性offsetRanges；有继承才能强转；
       offsetRanges是OffsetRange的数组，OffsetRange有几个属性，topic,partition,开始和结束offset
       */
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对DS的结构进行转换   ConsumerRecord[k,v] ===>value:jsonStr ==>OrderInfo，并补充天和小时字段
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        //获取json格式字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        //2020-10-27 14:30:20
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        //日期
        orderInfo.create_date = createTimeArr(0)
        //小时
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
    //orderInfoDStream.print(1000)
    /*
    //===================2.判断是否为首单  ====================
    //方案1   对于每条订单都要执行一个sql，sql语句过多
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //获取用户id
        val userId: Long = orderInfo.user_id
        //根据用户id到Phoenix中查询是否下单过
        var sql: String = s"select user_id,if_consumed from user_status0523 where user_id='${userId}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
    orderInfoWithFirstFlagDStream.print(1000)
    */

    //===================2.判断是否为首单  ====================
    //方案2  以分区为单位，将整个分区的数据拼接一条SQL进行一次查询。避免频繁连接和断开phoenix
    //使用mapPartitions，一个分区的一条条数据会组成可迭代，而且用一次就会被内存释放。
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        /*当前一个分区中所有的订单数据，从可迭代转换为List集合，为什么转换？？？
        因为可迭代用一次就会被内存释放，像以下就对数据使用了两次，一次是map，一次是遍历数据并写入是否是首单。
        分区中所有数据是流DStream，是一个个的OrderInfo组成的iterable,不好操作，先转成scala的List，容易操作
         */
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList

        /*
        获取当前分区中获取下订单的用户的id的集合List，也就是List中只留下user_id,
        从List[OrderInfo] => List[user_id]
        map{OrderInfo => OrderInfo.user_id}的简写
         */
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        //根据用户集合到Phoenix中查询，看看哪些用户下过单   坑1  字符串拼接以','分割；因为id不是Int，是字符串，所以会有单引号‘
        //userIdList.mkString("','")，将List中的元素，以','分割，形成string
        var sql: String =
          "select user_id,if_consumed from user_status0523 where user_id in('${userIdList.mkString("','")}')"

        //执行sql从Phoenix获取数据
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的用户id   坑2 要用大写"USER_ID"
        //因为返回的userStatusList是一个JSONObject组成的List,
        // 所以要取出用户id要用getString，这样就把所有用户id放一起成List[USER_ID]
        //map{JSONObject => JSONObject.getString("USER_ID")}的简写
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))

        for (orderInfo <- orderInfoList) {
          //坑3    类型转换,user_id是Long类型要转String
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        //再转成可迭代的
        orderInfoList.toIterator
      }
    }
    //orderInfoWithFirstFlagDStream.print(1000)


    /*
    ===================4.同一批次中状态修正  ====================
    用户有可能在一个采集周期5s内下两次单，如果不修正，可能两个都是首单
    应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
    	同一采集周期的同一用户-----按用户分组（groupByKey）
    	最早的订单-----排序，取最早（sortwith）
    	标记为首单-----具体业务代码
    */
    //对待处理的数据进行结构转换orderInfo====>(userId,orderInfo)；
    // 必须转换成二元组，因为要按key分组
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map(orderInfo=>(orderInfo.user_id,orderInfo))
    //根据用户id对数据进行分组，同一用户的几个OrderInfo放一起，组成可迭代
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()

    /*
    使用扁平化后再映射，flatMap，将流中的数据散开；
    为什么用flatmap??? 因为同一个userId，可能对应多个orderInfo；扁平化就是将所有的数据展开，
    从(userId,orderInfoItr可迭代) => (userId, orderInfo)
    从二元组(userId, orderInfoItr)，最后返回orderInfoList[OrderInfo]；key已在上面用于排序后不用保留
     */
    val orderInfoRealDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //判断在一个采集周期中，同一用户（前面已按用户id排序）是否下了多个订单
        if (orderInfoList != null && orderInfoList.size > 1) {
          //如果下了多个订单，按照下单时间，对整个list里的同一用户的所有OrderInfo对象升序排序
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //取出集合中的第一个元素，就是最早的订单
          if (sortedOrderInfoList(0).if_first_order == "1") {
            //时间最早的订单首单状态保留为1，其它的都设置为非首单，下标从1开始
            for (i <- 1 until sortedOrderInfoList.size) {
              sortedOrderInfoList(i).if_first_order = "0"
            }
          }
          sortedOrderInfoList
        } else {
          orderInfoList
        }
      }
    }

    //===================5.和省份维度表进行关联====================
    /*
    //5.1 方案1：以分区为单位，对订单数据进行处理，和Phoenix中的订单表进行关联
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.mapPartitions {
      orderInfoItr => {
        //转换为List
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中订单对应的省份id
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //根据省份id到Phoenix中查询对应的省份
        var sql: String = s"select id,name,area_code,iso_code from gmall0523_province_info where id in('${provinceIdList.mkString("','")}')"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)

        //List不方便查询获取数据，转为map，key是省份id，value是ProvinceInfo对象
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            //将json对象转换为省份样例类对象
            //!!!注意转换方法是JSON.toJavaObject()
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        //对订单数据进行遍历，用遍历出的省份id ，从provinceInfoMap获取省份对象
        for (orderInfo <- orderInfoList) {
        //从map中取数据，要注意如果获取不到就设为Null
          val proInfo: ProvinceInfo = provinceInfoMap.getOrElse(orderInfo.province_id.toString, null)
          if (proInfo != null) {
            orderInfo.province_name = proInfo.name
            orderInfo.province_area_code = proInfo.area_code
            orderInfo.province_iso_code = proInfo.iso_code
          }
        }
        orderInfoList.toIterator
      }
    }
    orderInfoWithProvinceDStream.print(1000)
    */


    //5.2 方案2  以采集周期为单位对数据进行处理，使用transform --->通过SQL将所有的省份查询出来
    //transform和foreachRDD的代码是在driver端执行的。一个采集周期执行一次。
    //如果driver的性能好，可以以采集周期为单位处理；不然以分区为单位处理。
    //要理解以采集周期是体现在哪里？是使用transform，里面rdd，还是在driver执行的，不是在executor，所以是整个采集周期的
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.transform {
      rdd => {
        //从Phoenix中查询所有的省份数据
        var sql: String = "select id,name,area_code,iso_code from gmall0523_province_info"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)

        //把provinceInfoList里面的数据从provinceJsonObj，映射成二元组，(id, provinceInf)；
        //再转成map方便查询
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            /*
            ！！！使用toJavaObject，将json对象转换为省份样例类对象ProvinceInfo
             */
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            //返回的是二元组，(id, provinceInf)，以便于查询
            (provinceInfo.id, provinceInfo)
          }
        }.toMap
        //！！！定义省份的广播变量，用sparkContext进行广播
        val bdMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        //对rdd里面的数据映射，就可以关联上省份维度数据
        rdd.map {
          orderInfo => {
            //id要转成String类型，使用value()取出广播变量中的值map
            val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            //返回关联了维度的对象
            orderInfo
          }
        }
      }
    }
    //orderInfoWithProvinceDStream.print(1000)


    //===================6.和用户维度表进行关联====================
    //以分区为单位对数据进行处理，每个分区拼接一个sql 到phoenix上查询用户数据
    //因为用户的数据量大，driver可能扛不往，所以不以采集周期为单位，而以分区为单位，使用mapPartitions
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        //转换为list集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取所有的用户id， orderInfo => orderInfo.user_id, map是对orderInfoList里的数据映射
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据id拼接sql语句，到phoenix查询用户
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall0523_user_info " +
          s"where id in ('${userIdList.mkString("','")}')"
        //当前分区中所有的下单用户
        val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            //返回二元组，方便转成map
            (userInfo.id, userInfo)
          }
            //转成map
        }.toMap

        for (orderInfo <- orderInfoList) {
          //注意orderInfo.user_id是long类型，要转为String
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }
        }

        orderInfoList.toIterator
      }
    }
    orderInfoWithUserInfoDStream.print(1000)


    //===================3.维护首单用户状态|||保存订单到ES中  ====================
    //如果当前用户为首单用户（第一次消费），那么我们进行首单标记之后，应该将用户的消费状态保存到Hbase中，等下次这个用户
    //再下单的时候，就不是首单了

    //!!!导包，隐式转换
    import org.apache.phoenix.spark._
    orderInfoWithUserInfoDStream.foreachRDD{
      rdd=>{
        //优化，缓存
        rdd.cache()

        //3.1 维护首单用户状态
        //将首单用户过滤出来。只有首单用户的状态才要更新，不是首单的，之前状态已有消费过的备注的
        //OrderInfo => OrderInfo.if_first_order=="1"
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order=="1")

        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
        //所以把RDD中的数据从orderInfo转成UserStatus对象
        val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }

        /*
        注意这个方法的rdd调用的，不是流
        把是否是用户首单的状态，通过phoenix保存到Hbase中
        注意saveToPhoenix这个方法的调用，可以把RDD直接保存入Hbase中
         */
        userStatusRDD.saveToPhoenix(
          "USER_STATUS0523",
          //列名组成的Seq，
          Seq("USER_ID","IF_CONSUMED"),
          //conf,导的是Hadoop的包，因为Hbase是基于hdfs的
          new Configuration,
          //zk的url，是Option中的Some类型
          Some("hadoop102,hadoop103,hadoop104:2181")
        )

        //3.2保存订单数据到ES中
        //以分区为单位，使用foreachPartition
        rdd.foreachPartition{
          orderInfoItr=>{
            /*
            orderInfoItr.toList是一个List,里面元素是orderInfo，所以要映射成二元组(orderInfo.id,orderInfo)，
            才能传给es，并保存；
            而且orderInfo.id是作为es的文档 id的，这样才能保证es插入操作的幂等性，不会有重复数据
             */
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo=>(orderInfo.id.toString,orderInfo))

            //获取当前日期，并格式化
            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall0523_order_info_" + dateStr)

            //3.4写回到Kafka
            for ((orderInfoId,orderInfo) <- orderInfoList) {
              /*
              把对象转为json字符串，因为kafka的send方法要传入的是String；
              而里面的SerializeConfig会递归地序列化对象和里面的所有属性。
               */
              MyKafkaSink.send("dwd_order_info",JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }
          }
        }

        //3.3提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
