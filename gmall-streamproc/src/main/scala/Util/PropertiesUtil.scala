package Util

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.log4j.Logger

/**
  * @author gxy
  *         根据name,
  *         从config.properties中拿到prop
  */
object PropertiesUtil {
  private val logger = Logger.getLogger("")
  private val config = ConfigFactory.load("config.properties")

  def getProp(name: String): String = {
    var value = ""
    try value = config.getString(name)
    catch {
      case ce: ConfigException =>
        logger.warn("No string  configuration setting found for key " + name)
        return value
    }
    value
  }

  def main(args: Array[String]): Unit = {
    val prop = PropertiesUtil.getProp("kafka.servers")
    System.out.println(prop)
  }
}