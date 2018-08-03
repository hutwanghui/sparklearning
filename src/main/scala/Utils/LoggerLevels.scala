package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging


/**
  * Created by hutwanghui on 2018/8/3.
  * email:zjjhwanhui@163.com
  * qq:472860892
  */
/**
  * 减少streaming的一些无用信息
  */
object LoggerLevels extends Logging {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
