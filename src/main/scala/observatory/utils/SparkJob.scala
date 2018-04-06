package observatory.utils

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql.SparkSession

class SparkJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

}
