import org.apache.spark.sql.SparkSession

object ProjetoScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(name = "Projeto Scala").getOrCreate()

    val juros = spark.read.json( path="hdfs://namnode:8020/user/marco/data/juros_selic/juros_selic.json")
    juros.collect()
    juros.write.mode(saveMode = "overwrite").parquet(path = "hdfs://namnode:8020/user/marco/projeto_scala")

    spark.stop()
  }
}
