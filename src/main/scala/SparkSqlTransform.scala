import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * SparkSql 三种类型转换 RDD、DataSet、DataFrame
  * RDD、DataSet 互相转换
  */
object SparkSqlTransform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlTransform")
    val spark: SparkSession = new SparkSession.Builder().config(conf).getOrCreate()
    //加入隐式转换包
    import spark.implicits._
    //创建rdd
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "bz", 16), (2, "jdb", 18)))
//    //rdd转df
//    val df: DataFrame = rdd.toDF("id", "name", "age")
//    //df转ds
//    val ds: Dataset[User] = df.as[User]
//    //ds转df
//    val df2: DataFrame = ds.toDF()
//    //df转rdd     ==>Row通过索引获取数据
//    val rdd2: RDD[Row] = df2.rdd
//    rdd2.foreach(row => {
//      println("id:" + row.getInt(0) + ",name:" + row.getString(1) + ",age:" + row.getInt(2))
//    })




    //rdd <==> ds
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDs: Dataset[User] = userRDD.toDS()
    val rdd3: RDD[User] = userDs.rdd
    rdd3.foreach(println)

  }

}

case class User(id: Long, name: String, age: Long)
