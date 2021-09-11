package com.huashao.gmall.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.huashao.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.huashao.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil,
  OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Author: huashao
  * Date: 2021/9/3
  * Desc: 从Kafka的DWD层，读取订单和订单明细数据
 *  要点：
 *        oderInfo流和orderDetail流双流Join；
 *        实付分摊；
 *        写入ClickHouse（使用rdd转df，再write()）；
 *        理解行动算子真正触发的地点，与多次行动操作要先缓存RDD
 *
  * 注意：如果程序数据的来源是Kafka，在程序中如果触发多次行动操作，应该进行缓存rdd.cache()
  */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    //===============1.从Kafka中获取数据================
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic,orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic,orderDetailGroupId)


    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsetMap!= null && orderInfoOffsetMap.size > 0){
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoOffsetMap,orderInfoGroupId)
    }else{
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoGroupId)
    }

    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsetMap!= null && orderDetailOffsetMap.size > 0){
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailOffsetMap,orderDetailGroupId)
    }else{
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailGroupId)
    }

    //从Kafka读取数据后，获取读后的偏移量
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //从Kafka读取数据后，获取读后的偏移量
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对流中的数据，由kv类型的consumerRecord,转为orderInfo对象
    val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
      record => {
        val orderInfoStr: String = record.value()
        //从json字符串转为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
        orderInfo
      }
    }

    //对流中的数据，由kv类型的consumerRecord,转为orderDetail对象
    val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        //从json字符串转为OrderDetail对象
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //===============2.双流Join================
    //开窗,滑动窗口，窗口长度20秒，步长5秒
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDS.window(Seconds(20),Seconds(5))

    val odrderDetaiWindowDStream: DStream[OrderDetail] = orderDetailDS.window(Seconds(20),Seconds(5))

    //要先开窗，再转为二元组
    //转换为kv结构，二元组(orderInfo.id, orderInfo)；因为双流join根据的就是相同的订单id才能合并
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      orderInfo => {
        (orderInfo.id, orderInfo)
      }
    }

    //转为二元组，(orderDetail.order_id, orderDetail)，因为双流join根据的就是相同的订单id才能合并
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = odrderDetaiWindowDStream.map {
      orderDetail => {
        (orderDetail.order_id, orderDetail)
      }
    }

    //双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)

    /*
    join的关键：同一个orderId的情况下，缓存orderDetailId，可以根据它去重；
    去重  Redis   type:set    key: order_join:[orderId]   value:orderDetailId  expire :600
    Set里面放的是orderDetailId，
    一个orderId, 可能对应多个orderDetailId。
    以分区为单位，建立redis连接；使用mapPartitions,它会把同一分区的所有数据，形成一个可迭代的集合
    使用Redis是因为保存时间短；
     */
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        //迭代器转为List，本身结构是(OrderId, (OrderInfo, OrderDetail))
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        //获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        val orderWideList = new ListBuffer[OrderWide]

        //遍历，里面每个元素是二元组(orderId, (orderInfo, orderDetail))
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join:" + orderId

          /*
          往redis里的结构set存orderDetail.id，不是orderId；
          orderId是key，用于标识set的，也就是同一个oderId会对应多个orderDetail.id
          而且最后redis中set的记录数，是join后的记录数，都是orderDetail.id的条数
          这一步就是缓存所有的orderDetail.id
          记得orderDetail.id是long类型要转换
           */
          val isNotExists: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          //设置过期时间是10分钟
          jedis.expire(orderKey, 600)

          if (isNotExists == 1L) {
            /*
            如果存成功，返回1，说明同一个orderId中之前这个orderDetail.id不存在，现在是首次，之前的orderDetail不存在，现在数据来了
            对应的就是，同一个oderInfo会对应多个orderDetail
            生成OrderWide对象，传入的参数是orderInfo, orderDetail对象；
            生成的对象再存入ListBuffer中
             */
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()

        //再转为可迭代，放入流
        orderWideList.toIterator
      }
    }
    //orderWideDStream.print(1000)

    //===============3.实付分摊================

    //以分区为单位，使用mapPartitions,它会把同一分区的所有数据，形成一个可迭代的集合
    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        //迭代器转为List，List里的数据是OrderWide对象
        val orderWideList: List[OrderWide] = orderWideItr.toList
        //获取Jedis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        for (orderWide <- orderWideList) {
          //3.1从Redis中获取 明细累加，用于判断是否是订单最后一条明细。
          var orderOriginSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          //注意：从Redis中获取字符串，都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            //取出的是String，要转为Double
            orderOriginSum = orderOriginSumStr.toDouble
          }

          //3.2从Reids中获取 实付分摊累加和，用于求订单中最后一条明细的实付金额，减法是避免最后一条有除不尽的小数赞成的差额。
          var orderSplitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          //该条明细orderWide (数量*单价)
          val detailAmount: Double = orderWide.sku_price * orderWide.sku_num

          //3.3判断是否为最后一条  计算实付分摊，  判断条件：该条明细 (数量*单价) == 原始总金额 -（其他明细 【数量*单价】的合计）
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            //实付分摊金额= 实付总金额 - (其他明细已经计算好的【实付分摊金额】的合计)
            //四舍五入Math.round()
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            //不是最后一条，实付分摊金额 = 实付总金额 * (数量 * 单价) / 原始总金额
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100) / 100d
          }

          //3.4更新Redis中的值
          //订单的明细累加和，要加上新的一个明细金额，
          var newOrderOriginSum = orderOriginSum + detailAmount

          //同样是存入redis中的set数据结构中；
          //key的有效期是10分钟
          jedis.setex(orderOriginSumKey, 600, newOrderOriginSum.toString)

          //订单的实付分摊累加和，要加上新的一个明细的实付金额
          var newOrderSplitSum = orderSplitSum + orderWide.final_detail_amount
          jedis.setex(orderSplitSumKey, 600, newOrderSplitSum.toString)
        }
        //关闭连接
        jedis.close()
        //再转为可迭代
        orderWideList.toIterator
      }
    }
    orderWideSplitDStream.print(1000)
    orderWideSplitDStream.cache()
    println("---------------------------------")
    orderWideSplitDStream.print(1000)


    /*
    !!!把Spark流中的数据保存到ClickHouse
    */
    //创建SparkSession对象，建造者模式
    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()

    //导入隐式转换，这里的spark是SparkSession，不是其他
    import spark.implicits._

    //对DS中的RDD进行处理，往ClickHouse保存一份，往Kafka里保存一份，都是行动算子
    orderWideSplitDStream.foreachRDD{
      rdd=>{
        //这里是坑！！： 这里的foreachRDD，还没真正执行行动算子。在这里缓存RDD。
        rdd.cache()
        //根据rdd,创建DataFrame
        val df: DataFrame = rdd.toDF

        //dataFrame通用的写出方法
        //默认的mode是报错，errorIfExist,这里用append
        //！！真正的执行行动算子的操作是这个write! 如果之后还要对rdd作操作，就要在write之前缓存rdd！！
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务隔离级别
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://hadoop102:8123/default",
            "t_order_wide_0523",new Properties())


        //将每个rdd里的orderWide对象写回到Kafka dws_order_wide
        rdd.foreach{
          orderWide=>{
            //把orderWide对象转为json字符串，并递归系列化它的属性
            MyKafkaSink.send("dws_order_wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }

        //提交偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
