package com.kk

import Utils.LoggerLevels
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hutwanghui on 2018/8/3.
  * email:zjjhwanhui@163.com
  * qq:472860892
  */
object WordCountStreaming {

  //批次累加
  //第一个参数：当前批次
  //第二个参数：以前的批次（因为有可能没有，比如第一次，使用Option）
  val func = (itemOld: Iterator[(String, Seq[Int], Option[Int])]) => {
    itemOld.flatMap {
      case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m))
    }
  }

  def main(args: Array[String]) {
    //设置日志级别,减少日志信息非必要侠士
    LoggerLevels.setStreamingLogLevels()

    //创建SparkConf并设置为本地模式运行
    //注意local[2]代表开两个线程,因为是使用了Socket的链接方式，所以必须要双核消息的生产者和消费者两个线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //设置DStream批次时间间隔为2秒
    val ssc = new StreamingContext(conf, Seconds(2))
    //做checkpoint 写入共享存储中
    ssc.checkpoint("/home/spark/streaming")
    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    //通过网络读取数据
    val lines = ssc.socketTextStream("172.16.0.15", 8190)
    //updateStateByKey结果可以累加但是需要传入一个自定义的累加函数：updateFunc
    val wordCounts = lines.flatMap(_.split(" ")).map(word => (word, 1)).updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    //打印结果到控制台
    wordCounts.print()
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }

}
