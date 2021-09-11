package com.huashao.gmall.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
  * Author: huashao
  * Date: 2021/8/25
  * Desc: 读取配置文件的工具类
  */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }

  def load(propertiesName:String): Properties ={
    val prop: Properties = new Properties()
    //加载指定的配置文件
    /*
    读取配置文件时，要使用类路径，不能使用绝对路径，因为以后项目打包后，并不在本地运行。
    怎么获取classLoader？
    使用classLoader的getResourceAsStream（）方法，可在类路径下读取配置文件 */
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    prop
  }
}
