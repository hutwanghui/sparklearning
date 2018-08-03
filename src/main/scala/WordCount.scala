package com.kk

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by msi- on 2018/7/4.
  * ./bin/spark-submit \
  * --class com.kk.WordCount \
  * --executor-memory 1G \
  * /home/wordCount-spark-1.0-SNAPSHOT.jar \
  * hdfs://master:9000/input/test.txt \
  * Shdfs://master:9000/out
  */
/*
 spark-submit --class com.kk.WordCount --executor-memory 200M --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8190" /home/spark/debug/sparklearning_2.11-1.0.jar /input/wordcount/test.txt /out
 */

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置APP名称
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    //创建sparkContext，该对象是提交spark app的入口
    val sc = new SparkContext(conf)
    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)
      .saveAsTextFile(args(1))
    //停止sc，结束该任务
    sc.stop()
  }
}
