package com.huashao.gmall.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.huashao.gmall.realtime.bean.OrderWide
import com.huashao.gmall.realtime.util.{MyKafkaUtil, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
  * Author: huashao
  * Date: 2021/9/6
  * Desc: 品牌统计应用程序，并把统计的每个品牌的交易总额和kafka读取后的偏移量，作为一个事务，存入mysql
 *  要点：
 *      根据key品牌Id_品牌名，汇总，所以将流中的数据转为二元组（品牌Id_品牌名，每个品牌的交易总额），再reduceByKey()
 *      所有数据要收集回driver端，才能单线程处理，rdd.collect();
 *      DB.localTX的使用，把处理数据并存入mysql，和保存偏移量放入同一个事务
  */
object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("TrademarkStatApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "ads_trademark_stat_group"
    val topic = "dws_order_wide"

    //从mysql中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic,groupId)

    //从Kafka读取数据，创建DStream
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0 ){
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取本批次消费的kafka数据情况
    //创建空的数据，使用empty
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStreasm: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //结构转换
    val orderWideDStream: DStream[OrderWide] = offsetDStreasm.map {
      record => {
        //转换为json格式字符串
        val jsonStr: String = record.value()
        /*
          JSON中用到的方法
            JSON.parseObject(jsonStr,classOf[类型])  将json字符串转换为对象
            JSON.toJavaObject(jsonObj,classOf[类型])  将json对象转换为对象
            JSON.toJSONString(对象,new SerializeConfig(true))  将对象转换为json格式字符串
        */
        //将json格式字符串转换为对应的样例类对象
        val orderWide: OrderWide = JSON.parseObject(jsonStr, classOf[OrderWide])
        orderWide
      }
    }

    //orderWideDStream.print(1000)

    //对流中的数据映射map，将流中的数据，从orderWide转成二元组（品牌Id_品牌名，每个品牌的交易总额）
    val orderWideWithAmountDStream: DStream[(String, Double)] = orderWideDStream.map {
      orderWide => {
        //以品牌Id_品牌名作为key，该条明细的实付金额为value,形成二元组
        (orderWide.tm_id + "_" + orderWide.tm_name, orderWide.final_detail_amount)
      }
    }

    //根据key品牌Id_品牌名来聚合，转成二元组就是为了这里的根据key来汇总
    val reduceDStream: DStream[(String, Double)] = orderWideWithAmountDStream.reduceByKey(_+_)

    //reduceDStream.print(1000)

    /*
    //累加值是一个采集周期里的，使用的是foreachRDD
    //将数据保存到MySQL中  方案1   单条插入
    reduceDStream.foreachRDD {
      rdd =>{
        // ！！！为了避免分布式事务，把ex的数据提取到driver中;因为做了聚合，所以可以直接将Executor的数据聚合到Driver端
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr !=null && tmSumArr.size > 0) {
          //读取配置文件
          DBs.setup()
          DB.localTx {
            implicit session =>{
              // 写入计算结果数据
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              for ((tm, amount) <- tmSumArr) {
                val statTime: String = formator.format(new Date())
                val tmArr: Array[String] = tm.split("_")
                val tmId = tmArr(0)
                val tmName = tmArr(1)
                val amountRound: Double = Math.round(amount * 100D) / 100D
                println("数据写入执行")
                SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                  .bind(statTime,tmId,tmName,amountRound).update().apply()
              }
              //throw new RuntimeException("测试异常")
              // 写入偏移量
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                println("偏移量提交执行")
                SQL("replace into offset_0523  values(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()
              }
            }
          }
        }
      }
    }*/

    //方式2：批量插入
    //累加值是一个采集周期里的，使用的是foreachRDD
    reduceDStream.foreachRDD {
      rdd =>{
        // ！！！为了避免分布式事务，把ex的数据提取到driver中;因为做了聚合，所以可以直接将Executor的数据聚合到Driver端
        //collect后返回的是一个个二元组组成的Array
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr !=null && tmSumArr.size > 0) {
          //读取配置文件
          DBs.setup()

          //多个sql组成一个事务
          DB.localTx {
                //隐式参数
            implicit session =>{
              // 写入计算结果数据
              //获取当前时间
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val dateTime: String = formator.format(new Date())

              //用于存放参数
              val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()

              //遍历收到到driver端的数据，是数组Array[(String, Double)]，
              // 里面每条数据是二元组（品牌Id_品牌名，每个品牌的交易总额）
              for ((tm, amount) <- tmSumArr) {
                //对每个品牌的交易总额进行四舍五入，保留2位小数
                val amountRound: Double = Math.round(amount * 100D) / 100D
                //因为品牌Id_品牌名是以_连接，所以以_分割
                val tmArr: Array[String] = tm.split("_")
                val tmId = tmArr(0)
                val tmName = tmArr(1)

                //把参数，放入集合Seq中，再存入ListBuffer。一个Seq是要插入mysql的一行数据的参数
                batchParamsList.append(Seq(dateTime, tmId, tmName, amountRound))
              }
              /*val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01 10:10:10",
              "101","品牌1",2000.00),
              Seq("2020-08-01 10:10:10","102","品牌2",3000.00))
              数据集合作为多个可变参数 的方法 的参数的时候 要加:_*，是固定写法；
              batch()里要传入的是集合，所以ListBuffer要转成集合
               */
              SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) " +
                "values(?,?,?,?)")
                .batch(batchParamsList.toSeq:_*).apply()

              //throw new RuntimeException("测试异常")

              // 写入偏移量，从offsetRanges中获取partitonId,结束的offset
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                SQL("replace into offset_0523  values(?,?,?,?)")
                  .bind(groupId, topic, partitionId, untilOffset)
                  .update().apply()
              }
            }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
