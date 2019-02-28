package spark.example.tutorial.chapter12

import spark.example.common.SparkHelper

import scala.util.Random

object LowLevelApiMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    // 12.1~
    val rdd1 = spark.range(500).rdd
    println(rdd1)

    val rdd2 = spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    rdd2.foreach(println)

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    words.setName("myWords")
    println(words.name)
    println(words.distinct().count())

    def startsWithS(individual: String) = {
      individual.startsWith("S")
    }
    val c = words.filter(word => startsWithS(word)).collect()
    c.foreach(println)

    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    words2.filter(record => record._3).take(5).foreach(println)

    words.flatMap(word => word.toSeq).take(5).foreach(println)
    words.sortBy(word => word.length() * -1).take(2).foreach(println)

    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

    println(spark.sparkContext.parallelize(1 to 20).reduce(_ + _)) //210

    def wordLengthReducer(leftWord: String, rightWord: String) = {
      if (leftWord.length > rightWord.length)
        leftWord
      else
        rightWord
    }
    words.reduce(wordLengthReducer).foreach(println)

    // 12.6
    val confidence = 0.95
    val timeoutMilliseconds = 400
    println(words.countApprox(timeoutMilliseconds, confidence))
    println(words.countApproxDistinct(4, 10))
    println(words.countByValue())
    println(words.countByValueApprox(1000, 0.95))
    println(words.first())

    println(spark.sparkContext.parallelize(1 to 20).max())
    println(spark.sparkContext.parallelize(1 to 20).min())

    words.take(5).foreach(println)
    words.takeOrdered(5).foreach(println)
    words.top(5).foreach(println)
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed).foreach(println)

    // 12.7
    val randomInt = new Random().nextInt()
    words.saveAsTextFile("file:/tmp/bookTitle" +  randomInt)

    import org.apache.hadoop.io.compress.BZip2Codec
    words.saveAsTextFile("file:/tmp/bookTitleCompressed" +  randomInt, classOf[BZip2Codec])

    words.saveAsObjectFile("/tmp/my/sequenceFilePath" + randomInt)

    // 12.8

    println(words.cache())
    println(words.getStorageLevel)

    //spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
    //words.checkpoint()

    // 12.10
    println(words.pipe("wc -l").collect())

    println(words.mapPartitions(part => Iterator[Int](1)).sum()) // 2

    def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
      withinPartIterator.toList.map(
        value => s"Partition: $partitionIndex => $value").iterator
    }
    words.mapPartitionsWithIndex(indexedFunc).collect().foreach(println)

    words.foreachPartition { iter =>
      import java.io._
      import scala.util.Random
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    }

    spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()


  }



}
