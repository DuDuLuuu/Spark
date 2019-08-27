import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark = new sql.SparkSession.Builder().config(conf).getOrCreate()
    //读取文件数据
    val frame: DataFrame = spark.read.json("in/1.json")
    //测试读取文件
    //    frame.show()

    //DataFrame 转化为视图
    frame.createTempView("user")
    //创建或替换
    //    frame.createOrReplaceTempView("user")
    //采用sql语法访问数据
    spark.sql("select * from user").show()

    //释放资源
    spark.stop()
  }
}
