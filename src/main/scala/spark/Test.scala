package spark

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkTest")
      .master("local[8]")
      .config("spark.executor.cores", "8")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()

    val t0 = System.currentTimeMillis()

    val sc = spark.sparkContext
    sc.parallelize(1 to 24, 24)
      .map(n => {
        for (i <- 1 to 10000)
          for (j <- 1 to 10000)
            i + j
        n
      }).count()

    println(System.currentTimeMillis() - t0)
  }
}
