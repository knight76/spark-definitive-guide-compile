package spark.example.tutorial.chapter13

import org.apache.spark.{SparkConf, SparkContext}
import spark.example.common.SparkHelper

object AdvancedLowLevelApiMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    words.map(word => (word.toLowerCase, 1)).foreach(println)

    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    keyword.mapValues(word => word.toUpperCase).collect().foreach(println)

    keyword.flatMapValues(word => word.toUpperCase).collect().foreach(println)

    keyword.keys.collect().foreach(println)
    keyword.values.collect().foreach(println)

    keyword.lookup("s").foreach(println)

    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct.collect()
    import scala.util.Random
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()
      .foreach(println)

    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L)
      .collect()
      .foreach(println)

    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))
    def maxFunc(left:Int, right:Int) = math.max(left, right)
    def addFunc(left:Int, right:Int) = left + right
    val nums = sc.parallelize(1 to 30, 5)

    val timeout = 1000L //ms
    val confidence = 0.95
    KVcharacters.countByKey().foreach(println)
    println(KVcharacters.countByKeyApprox(timeout, confidence))

    KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()

    KVcharacters.reduceByKey(addFunc).collect()

    nums.aggregate(0)(maxFunc, addFunc)

    val depth = 3
    nums.treeAggregate(0)(maxFunc, addFunc, depth)

    KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()

    //
    val valToCombiner = (value:Int) => List(value)
    val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2

    val outputPartitions = 6
    KVcharacters
      .combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions)
      .collect()
      .foreach(println)

    KVcharacters.foldByKey(0)(addFunc).collect().foreach(println)

    import scala.util.Random
    val distinctCharss = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val charRDD = distinctCharss.map(c => (c, new Random().nextDouble()))
    val charRDD2 = distinctCharss.map(c => (c, new Random().nextDouble()))
    val charRDD3 = distinctCharss.map(c => (c, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).take(5).foreach(println)

    val keyedChars = distinctCharss.map(c => (c, new Random().nextDouble()))
    val outputPartitionss = 10
    KVcharacters.join(keyedChars).count()
    KVcharacters.join(keyedChars, outputPartitionss).count()

    //
    println("--")
    val numRange = sc.parallelize(0 to 9, 2)
    words.zip(numRange).collect().foreach(println)

    println(words.coalesce(1).getNumPartitions) //1
    words.repartition(10)

    //
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("origin-source/data/retail-data/all/")
    val rdd = df.coalesce(10).rdd
    rdd.take(5).foreach(println)

    df.printSchema()

    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)

    import org.apache.spark.HashPartitioner
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10)

    //
    import org.apache.spark.Partitioner
    class DomainPartitioner extends Partitioner {
      def numPartitions = 3
      def getPartition(key: Any): Int = {
        val customerId = key.asInstanceOf[Double].toInt
        if (customerId == 17850.0 || customerId == 12583.0) {
          return 0
        } else {
          return new java.util.Random().nextInt(2) + 1
        }
      }
    }

    keyedRDD
      .partitionBy(new DomainPartitioner)
      .map(_._1)
      .glom()
      .map(_.toSet.toSeq.length)
      .take(5)
      .foreach(println)


    class SomeClass extends Serializable {
      var someValue = 0
      def setSomeValue(i:Int) = {
        someValue = i
        this
      }
    }

    spark.sparkContext.parallelize(1 to 10)
      .map(num => new SomeClass().setSomeValue(num))
      .foreach(_.someValue)

//    val conf = new SparkConf().setMaster("local[1]").setAppName("kyro")
//    conf.registerKryoClasses(Array(classOf[SomeClass], classOf[SomeClass]))
//    val scc = new SparkContext(conf)

  }
}
