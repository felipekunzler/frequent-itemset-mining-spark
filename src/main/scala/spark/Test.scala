package spark

import bloomfilter.mutable.BloomFilter
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder()
      .appName("SparkTest")
      .master("local[8]")
      .config("spark.executor.cores", "8")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()

    val t0 = System.currentTimeMillis()

    val sc = spark.sparkContext*/
    /*sc.parallelize(1 to 24, 24)
      .map(n => {
        for (i <- 1 to 10000)
          for (j <- 1 to 10000)
            i + j
        n
      }).count()*/

    val bf = BloomFilter[String](1000, 0.1)
    for (i <- 1 to 1000) {
      bf.add(i.toString)
    }

    var count = 0
    for (i <- 1001 to 2000) {
      val contains = bf.mightContain(i.toString())
      if (contains) count += 1
    }
    println(count)

    //println(System.currentTimeMillis() - t0)
  }
}
