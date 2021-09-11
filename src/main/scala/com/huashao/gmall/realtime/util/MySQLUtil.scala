package com.huashao.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Author: huashao
  * Date: 2021/9/6
  * Desc: 从MySQL中查询数据的工具类
  */
object MySQLUtil {

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from offset_0523")
    println(list)
  }

  //查询方法
  def queryList(sql:String): List[JSONObject] ={

    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    //建立连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop102:3306/gmall0523_rs?characterEncoding=utf-8" +
        "&useSSL=false",
      "root",
      "hua3293316")

    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()

    //获取结果集的元数据，用于获取结果集里每一条记录的列数，列名
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val userStatusJsonObj = new JSONObject()
      for(i <-1 to rsMetaData.getColumnCount){
        //把结果集里的数据，封装入JSONObject中，而不是普通bean
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      //再把JSONObject放入ListBuffer中
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()

    //ListBuffer转为List并作为返回值
    rsList.toList
  }
}
