import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, sql}

/**
  * spark对数据库操作
  */
object SparkDb {
  def main(args: Array[String]): Unit = {

    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark = new sql.SparkSession.Builder().config(conf).getOrCreate()

    import spark.implicits._
    //    ================read========================
    //方式一
    //    val df1: DataFrame = spark.read.format("jdbc")
    //      .option("url", "jdbc:oracle:thin:@192.168.1.168:1521:orcl")
    //      .option("dbtable", "SYS_USER")
    //      .option("user", "d_wq_trade")
    //      .option("password", "d_wq_trade_123")
    //      .load()
    //    df1.show()
    //方式二
    //    val properties = new Properties()
    //    properties.put("user", "zxxoa")
    //    properties.put("password", "d_zxxoa_123")
    //    val df2: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.1.246:3306/zxxoa", "SYS_USER", properties)
    //    df2.show()


    //    ================write========================

    //方式一
    //    val df: DataFrame = spark.sparkContext.makeRDD(List((1, "bz", 18))).toDF("id", "name", "age")
    //    df.write
    //      .format("jdbc")
    //      .option("url", "jdbc:oracle:thin:@192.168.1.168:1521:orcl")
    //      .option("dbtable", "SYS_USER")
    //      .option("user", "d_wq_trade")
    //      .option("password", "d_wq_trade_123")
    //      .save()
    //方式二
    //    df.write
    //      .jdbc("jdbc:oracle:thin:@192.168.1.168:1521:orcl", "SYS_USER", properties)


    spark.stop()
  }
}
