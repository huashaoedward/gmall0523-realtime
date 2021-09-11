package com.huashao.gmall.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
/**
  * Author: huashao
  * Date: 2021/8/24
  * Desc: 操作ES的客户端工具类
  */
object MyESUtil {

  //声明Jest客户端工厂
  private var jestFactory:JestClientFactory = null

  //提供获取Jest客户端的方法
  def getJestClient(): JestClient ={
    if(jestFactory == null){
      //创建Jest客户端工厂对象的配置信息
      build()
    }
    //真正地从工厂创建jest客户端对象
    jestFactory.getObject
  }

  //创建Jest客户端工厂，并配置信息
  def build(): Unit = {
    jestFactory = new JestClientFactory
    //使用建造者模式，配置jest工厂的信息
    jestFactory.setHttpClientConfig(new HttpClientConfig
        .Builder("http://hadoop102:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(1000).build())
  }

  //向ES中插入单条数据  方式1  将插入文档的数组以json的形式直接传递
  def putIndex1(): Unit ={
    //获取客户端连接
    val jestClient: JestClient = getJestClient()
    //定义执行的source，表示要插入到索引中的文档，底层会转换Json格式的字符串
    var source:String =
      """
        |{
        |  "id":200,
        |  "name":"operation meigong river",
        |  "doubanScore":8.0,
        |  "actorList":[
        |	    {"id":3,"name":"zhang han yu"}
        |	  ]
        |}
      """.stripMargin
    //创建插入类 Index   Builder中的参数表示 要插入到索引中的文档，底层会转换Json格式的字符串，所以也可以将文档封装为样例类对象
    val index:Index = new Index.Builder(source)
      .index("movie_index_5")
      //type有飘号，类似关键字
      .`type`("movie")
      //文档的id
      .id("1")
      .build()
    //通过客户端对象操作ES     execute参数为Action类型，Index是Action接口的实现类
    jestClient.execute(index)
    //关闭连接
    jestClient.close()
  }

  //向ES中插入单条数据  方式2   将向插入的文档封装为一个样例类对象
  def putIndex2(): Unit ={
    val jestClient = getJestClient()

    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String,Any]]()
    val actorMap1: util.HashMap[String, Any] = new util.HashMap[String,Any]()
    actorMap1.put("id",66)
    actorMap1.put("name","李若彤")
    actorList.add(actorMap1)

    //封装样例类对象
    val movie: Movie = Movie(300,"天龙八部",9.0f,actorList)
    //创建Action实现类 ===>Index
    //！！！底层会把movie样例类，转成json字符串
    val index: Index = new Index.Builder(movie)
      .index("movie_index_5")
      //type有飘号，类似关键字
      .`type`("movie")
      //文档的id
      .id("2").build()
    jestClient.execute(index)
    jestClient.close()
  }

  //根据文档的id，从ES中查询出一条记录
  def queryIndexById(): Unit ={
    val jestClient = getJestClient()
    val get: Get = new Get.Builder("movie_index_5","2").build()
    //get是负责查询功能的Action的实现类，查询后的结果是json
    val res: DocumentResult = jestClient.execute(get)
    //res.getJsonString获取json字符串
    println(res.getJsonString)
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档  方式1
  def queryIndexByCondition1(): Unit ={
    val jestClient = getJestClient()
    var query:String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "天龙"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "李若彤"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    //封装Search对象，是Action的实现类
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    val res: SearchResult = jestClient.execute(search)
    /*
    多条查询结果，每条结果用map封装，多条结果再一起放入list中。
    res是查询结果集，一个Json对象，getHits是取出命中的数据，Hits这一个对象里的数据。
    getHits()要传入T，命中的数据里有一个[]，里面是多个{}，而T是用于描述{}里的kv内容，这里我们使用map来描述kv对；
    Hit[util.Map[String, Any], Void]是其中一条命中记录，也是Kv对，map是它的k，已包装完信息，不需要v，所以用void。
    */
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])
    //将java的List转换为json的List
    import scala.collection.JavaConverters._
    //把list转为map，map里面有多个数据，只取source这个数据，再转成List
    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    //list转为字符串，用换行分隔
    println(resList1.mkString("\n"))

    jestClient.close()
  }


  //根据指定查询条件，从ES中查询多个文档  方式2
  def queryIndexByCondition2(): Unit ={
    val jestClient = getJestClient()
    //SearchSourceBuilder用于构建查询的json格式字符串
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","李若彤"))
    searchSourceBuilder.query(boolQueryBuilder)
    //开始位置
    searchSourceBuilder.from(0)
    //显示记录的条数
    searchSourceBuilder.size(10)
    //排序字段，长序
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    //searchSourceBuilder要转成String？
    val query: String = searchSourceBuilder.toString
    //println(query)

    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val res: SearchResult = jestClient.execute(search)
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])

    import scala.collection.JavaConverters._
    val list = resList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    queryIndexByCondition2()
  }

  /**
    * 向ES中批量插入数据
    * @param infoList  要存储的数据，是List[(String,Any)]
    * @param indexName  索引名
    */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if(infoList!=null && infoList.size!= 0){
      //获取客户端
      val jestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      //遍历传入的数据infoList，每一条元素的数据结构是二元组（mid,dauInfo对象）
      for ((id,dauInfo) <- infoList) {
        //遍历的每一个二元组中的dauInfo样例类，放入Index
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          //！！！就是这个文档id，在put方法中传入，保证了put的幂等性。如果不传，系统只会生成一个id
          //这个文档id对应的是mid
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      println("向ES中插入"+bulkResult.getItems.size()+"条数据")
      jestClient.close()
    }
  }
}

//样例类
//json中的数组[]可以对应java的List ,json中的{}可以对应map。
//map中的泛型是Any,不能用Object，Object类似scala中的any ref
case class Movie(id:Long,name:String,doubanScore:Float,actorList:util.List[util.Map[String,Any]]){}

