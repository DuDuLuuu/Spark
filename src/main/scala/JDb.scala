import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * spark core 中的三种数据结构 RDD、累加器、广播变量
  */
object JDb {
  //配置信息
  val conf = new SparkConf().setAppName("JDb").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //rdd
  val arrRDD: RDD[Int] = sc.makeRDD(1 to 10)
  val listRDD: RDD[Int] = sc.makeRDD(List(5, 7, 3, 4, 1, 6, 2, 8, 9, 10))
  val arrRDDPart3: RDD[Int] = sc.makeRDD(1 to 10, 3)
  val arrListRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4), List(5, 6)))
  val rdd1 = sc.parallelize(Array("coffe", "coffe", "panda", "monkey", "tea"))
  val rdd2 = sc.parallelize(Array("coffe", "monkey", "kitty"))
  //kv 类型rdd
  val kvRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (3, "dddd")))
  val kvRDD2: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
  val kvRDD3: RDD[(String, Int)] = sc.parallelize(List(("a", 32), ("a", 22), ("c", 44), ("b", 33), ("c", 66), ("c", 88)), 2)

  //打印RDD
  def printRDD[T](rdd: RDD[T]): Unit = {
    val rs: RDD[String] = rdd.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map(_ + "分区号：" + num)
      }
    }
    rs.collect().foreach(println(_))
  }
  //打印数组
  def printArrayRDD[T](rdd: RDD[Array[T]]): Unit = {
    rdd.collect().foreach(array => println(array.mkString(",")))
  }
  //打印kv类型RDD
  def printKvRdd[K, V](rdd: RDD[(K, V)]): Unit = {
    rdd.collect().foreach(println(_))
  }


  def main(args: Array[String]): Unit = {
    //    【WordCount】
    //    val lines: RDD[String] = sc.textFile("in")
    //    val line: RDD[String] = lines.flatMap(_.split(" "))
    //    val word: RDD[(String, Int)] = line.map((_, 1))
    //    val rs: RDD[(String, Int)] = word.reduceByKey(_ + _)
    //    rs.collect().foreach(println(_))
    //    listRDD.collect().foreach(println(_))

    //=============value算子==============
    //      【map】=>每个元素调用函数生成新的RDD
    //        printRDD(arrRDD.map(_ * 2))
    //      【mapPartitions】=>在Driver里把元素按分区发送给Excuter计算，性能比map好，但存在OOM风险
    //        val rs: RDD[Int] = arrRDD.mapPartitions(datas => {
    //          datas.map(_ * 3)
    //        })
    //        printRDD(rs)
    //      【mapPartitionsWithIndex】=>类似mapPartitions，带出分区号
    //        val rs: RDD[String] = arrRDD.mapPartitionsWithIndex {
    //          case (num, datas) => {
    //            datas.map(_ + "分区号：" + num)
    //          }
    //        }
    //        printRDD(rs)
    //      【flatMap】=>每个元素映射成0或多个元素
    //        printRDD(arrListRDD.flatMap(datas => datas))
    //      【glom】=>把同个分区的数据放在一个数组里面
    //        printArrayRDD(arrRDDPart3.glom())
    //      【groupBy】=>按函数的规则分组，key为函数返回值
    //        val rs: RDD[(Int, Iterable[Int])] = arrRDD.groupBy(_%2)
    //        rs.collect().foreach(println(_))
    //      【filter】=>每个元素调用函数结果为true的保留并生成新的RDD
    //       val rs: RDD[Int] = arrRDD.filter(_%2==0)
    //       printRDD(rs)
    //      【sample】=>随机抽样        参数(是否放回，分数值，随机种子)
    //       printRDD(arrRDD.sample(false,0.4,1))
    //      【distinct】=>去重
    //        printRDD(rdd1.distinct())
    //      【coalesce】=>缩减分区数量，提高小数据集执行效率       参数(分区数，是否进行suffer)默认不
    //       val rs: RDD[Int] = arrRDDPart3.coalesce(2)
    //       println(rs.partitions.size)
    //        printArrayRDD(rs.glom())
    //      【repartition】=>对coalesce封装，默认启用suffer
    //        println("repartition前分区数："+arrRDDPart3.partitions.size)
    //        val rs: RDD[Int] = arrRDDPart3.repartition(2)
    //        println("repartition后分区数："+rs.partitions.size)
    //        printArrayRDD(rs.glom())
    //      【sortBy】=>排序        参数(排序规则函数，aes顺序，分区数)
    //        val rs: RDD[Int] = arrRDD.sortBy(x => x, false, 4)
    //        println("分区数：" + rs.partitions.size)
    //        printRDD(rs)
    //      【union】=>俩RDD做并集计算，
    //        printRDD(rdd1.union(rdd2))
    //      【intersection】=>俩RDD做交集计算
    //        printRDD(rdd1.intersection(rdd2))
    //      【subtract】=>俩RDD做差集计算
    //        printRDD(rdd1.subtract(rdd2))
    //=============K-V算子==============
    //      【partitionBy】=>根据key分区
    //        println("分区数：" + kvRDD.partitions.size)
    //        val rs: RDD[(Int, String)] = kvRDD.partitionBy(new HashPartitioner(2))
    //        println("分区数：" + rs.partitions.size)
    //        printKvRdd(rs)
    //      【groupByKey】=>根据key分组
    //        println("分区数：" + kvRDD.partitions.size)
    //        val rs: RDD[(Int, Iterable[String])] = kvRDD.groupByKey()
    //        println("分区数：" + rs.partitions.size)
    //        printKvRdd(rs)
    //      【reduceByKey】=>根据key分组  相比groupByKey存在预聚合行为，性能好
    //        println("分区数：" + kvRDD.partitions.size)
    //        val rs: RDD[(Int, String)] = kvRDD.reduceByKey((k, v) => v+v, 2)
    //        println("分区数：" + rs.partitions.size)
    //        printKvRdd(rs)
    //      【aggregateByKey】=>参数(计算初始值)((分区内计算函数,首次计算会用到计算初始值),分区间计算函数)
    //        val rs: RDD[(String, Int)] = kvRDD2.aggregateByKey(0)((_ + _), _ + _)
    //        printKvRdd(rs)
    //      【foldByKey】=>参数(计算初始值)((分区内计算函数,首次计算会用到计算初始值),分区间计算函数)
    //        printKvRdd(kvRDD2.foldByKey(0)(_ + _))
    //      【combineByKey】=>参数((计算初始值),(分区内计算函数,首次计算会用到计算初始值),分区间计算函数)
    //        printKvRdd(kvRDD2.combineByKey(x => x, _ + _, _ + _))
    //      【sortByKey】=>根据key排序
    //        printKvRdd(kvRDD2.sortByKey(false))
    //      【mapValues】=>遍历键值对，只对value做操作
    //        printKvRdd(kvRDD2.mapValues(_+".png"))
    //      【join】=>根据key产生笛卡尔积元组
    //        printKvRdd(kvRDD2.join(kvRDD3))
    //      【cogroup】=>根据key,RDD 分组
    //        printKvRdd(kvRDD2.cogroup(kvRDD3))
    //=============action动作==============
    //      【reduce(func)】=>聚合，对RDD中元素执行func
    //        println(arrRDD.reduce(_ + _))
    //      【collect()】=>RDD收集成Array
    //        val arr: Array[(Int, Int)] = arrRDD.map(x => (x, 1)).collect
    //        arr.foreach(println(_))
    //      【count()】=>计算RDD元素数量
    //        println(arrRDD.count())
    //      【first()】=>获取RDD中的第一个元素
    //        println(arrRDD.first())
    //      【take(n)】=>获取RDD中的前n个元素
    //        println(arrRDD.take(3).mkString(","))
    //      【takeOrdered(n)】=>排序并获取RDD中的前n个元素
    //        println(listRDD.takeOrdered(4).mkString(","))
    //      【aggregate】=>计算(初始值[分区内、分区间，都会用到!!!])(分区内计算函数，分区间计算函数)
    //        println(arrRDDPart3.aggregate(0)(_+_,_+_))
    //        println(arrRDDPart3.aggregate(10)(_+_,_+_))
    //      【fold(num)(func)】=>折叠计算，封装aggregate，分区内、分区间 同一个计算函数
    //        println(arrRDDPart3.fold(10)(_ + _))
    //      【saveAsTextFile(path)】=>文件方式保存
    //        arrRDD.saveAsTextFile("output/text")
    //      【saveAsSequenceFile(path)】=>
    //      【saveAsObjectFile(path)】=>对象方式保存
    //        arrRDD.saveAsObjectFile("output/object")
    //      【countByKey()】=>根据key计算数量
    //        arrRDD.map((_,1)).countByKey().foreach(println(_))
    //      【foreach(func)】=>遍历
    //        arrRDD.foreach(println(_))
  }

}
