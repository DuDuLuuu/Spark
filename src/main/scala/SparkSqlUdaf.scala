import org.apache.spark
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * 用户自定义强类型聚合函数
  */
object SparkSqlUdaf {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlUdaf")
    val spark: SparkSession = new SparkSession.Builder().config(conf).getOrCreate()

    import spark.implicits._
    //创建实例
    val function = new MyAgeAvgClassFunction
    //聚合函数转换为查询列
    val column: TypedColumn[UserData, Double] = function.toColumn.name("avgAge")
    //调用自定义聚合函数
    val df: DataFrame = spark.read.json("in/1.json")
    val ds: Dataset[UserData] = df.as[UserData]

    val rs: Dataset[Double] = ds.select(column)
    rs.show()
//    println("平均年龄:"+rs)
    spark.stop()
  }
}

case class UserData(id: Long, name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Int)


class MyAgeAvgClassFunction extends Aggregator[UserData, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //计算
  override def reduce(b: AvgBuffer, a: UserData): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b2.count
    b1
  }

  //输出
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}