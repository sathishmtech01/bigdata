import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object merging {
def main(arg: Array[String]) {
val sparkConf = new SparkConf().setAppName("merging")
val sc = new SparkContext(sparkConf)
val sqlContext = new SQLContext(sc)
val input=arg(0)
val output=arg(1)
val data = sqlContext.read.json(input)
data.repartition(1).save(output,"json")
}
}
