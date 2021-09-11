package com.huashao.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Author: huashao
  * Date: 2021/8/30
  * Desc:  用于从Phoenix中查询数据
 *
 *  要点：查询的每条记录，要往jsonObj里面存放，再放入ListBuffer，最后返回时要转为List[JSONObject]
 *
  * User_id     if_consumerd
  *   zs            1
  *   ls            1
  *   ww            1
  *
  *  期望结果：
  *  {"user_id":"zs","if_consumerd":"1"}
  *  {"user_id":"zs","if_consumerd":"1"}
  *  {"user_id":"zs","if_consumerd":"1"}
*/
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status0523")
    println(list)
  }

  //要求返回值是一个List，里面元素是JSONObject
  def queryList(sql:String): List[JSONObject] ={

    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //建立连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()

    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val userStatusJsonObj = new JSONObject()
      //{"user_id":"zs","if_consumerd":"1"}
      //结果集RS和元数据的下标是从1开始，
      //往JSONObject里添加数据是put
      for(i <-1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()

    //将ListBuffer转换成List，再返回
    rsList.toList
  }
}
