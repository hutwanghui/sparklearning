import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by msi- on 2018/7/5.
  * ./bin/spark-submit \
  * --class GroceriesFP \
  * --executor-memory 1G \
  * /root/sparklearning_2.11-1.0.jar \
  * 3 0.1
  *
  */
object GroceriesFP {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置APP名称
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://master:7077")
    //创建sparkContext，该对象是提交spark app的入口
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:////root/Groceries.txt")
      .filter(line => !line.contains("items"))
      .map(line => line.split("\\{"))
      .map(s => s(1).replace("}\"", ""))

    //将购物数据按逗号拆分，转换成建模数据。并将其cache到内存中，方便我们多次使用该数据。
    val fpdata = data.map(_.split(",")).cache
    //FPGrowth模型建立,实例化FPGrowth并设置支持度为0.05，不满足这个支持度的数据将被去除和分区为3
    val fpGroup = new FPGrowth().setMinSupport(0.05).setNumPartitions(3)
    //开始创建模型，run方法用于FPGrowth算法计算频繁项集合
    val fpModel = fpGroup.run(fpdata)
    val freqItems = fpModel.freqItemsets.collect.sortWith((left, right) => left.freq > right.freq)
    //打印FrequentItem:频繁项,OccurrenceFrequency:出现次数
    freqItems.foreach(f => println("频繁项:" + f.items.mkString(",") + "  出现次数:" + f.freq))
    //构建FP树
    val userID: Int = args(0).toInt
    var userGoodfreq = 0L
    val userGoodsList = fpdata.take(userID + 1)(userID)
    for (good <- freqItems) {
      if (good.items.mkString == userGoodsList.mkString) {
        userGoodfreq = good.freq
      }
    }
    var renum = 0
    for (f <- freqItems) {
      if (f.items.mkString.contains(userGoodsList.mkString) && f.items.size > userGoodsList.size) {
        val result: Double = f.freq.toDouble / userGoodfreq.toDouble
        if (result >= args(1).toDouble) {
          var item = f.items

          for (i <- 0 until userGoodsList.size) {
            item = item.filter(_ != userGoodsList(i))
          }
          println("根据用户购买的" + userGoodsList.mkString + "进行如下推荐：")
          for (str <- item) {
            renum = renum + 1
            println("推荐商品：" + str + " 置信度： " + result)
          }
        }
      }
    }
    if (renum == 0) {
      println("没有推荐，将频繁项集里面的出现次数最高的5件推荐给用户")
      freqItems.take(5).foreach(f => println("推荐商品:" + f.items.mkString(",") + "  出现次数:" + f.freq))
    }
  }

}
