package observatory

import java.time.LocalDate

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import utils.Resources.resourcePath
import utils.SparkJob

/**
 * 1st milestone: data extraction
 */
object Extraction extends SparkJob {

  import spark.implicits._

  def stations(stationFile: String): Dataset[Station] = {
    spark
        .read
        .csv(resourcePath(stationFile))
        .select(
          concat_ws("~", coalesce('_c0, lit("")), '_c1).alias("id"),
          '_c2.alias("latitude").cast(DoubleType),
          '_c3.alias("longitude").cast(DoubleType)
        )
        .where('_c2.isNotNull && '_c3.isNotNull && '_c2 =!= 0.0 && '_c3 =!= 0.0)
        .as[Station]
  }

  def temperatures(year: Int, temperaturesFile: String): Dataset[TemperatureRecord] = {
    spark
        .read
        .csv(resourcePath(temperaturesFile))
        .select(
          concat_ws("~", coalesce('_c0, lit("")), '_c1).alias("id"),
          '_c2.alias("day").cast(IntegerType),
          '_c3.alias("month").cast(IntegerType),
          lit(year).as("year"),
          (('_c4 - 32) / 9 * 5).alias("temperature").cast(DoubleType)
        )
        .where('_c4.between(-200, 200))
        .as[TemperatureRecord]
  }

  def joined(
      stations: Dataset[Station],
      temperature: Dataset[TemperatureRecord]): Dataset[JoinedFormat] = {
    stations
        .join(temperature, usingColumn = "id")
        .as[Joined]
        .map(j => (StationDate(j.day, j.month, j.year), Location(j.latitude, j.longitude), j.temperature))
        .toDF("date", "location", "temperature")
        .as[JoinedFormat]
  }

  /**
   * @param year             Year number
   * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
   * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
   * @return A sequence containing triplets (date, location, temperature)
   */
  def locateTemperatures(
      year: Year,
      stationsFile: String,
      temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val j = joined(stations(stationsFile), temperatures(year, temperaturesFile))
    j.collect()
        .par
        .map(
          jf => (jf.date.toLocaleDate, jf.location, jf.temperature)
        ).seq
  }

  /**
   * @param records A sequence containing triplets (date, location, temperature)
   * @return A sequence containing, for each location, the average temperature over the year.
   */
  def locationYearlyAverageRecords(
      records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
        .par
        .groupBy(_._2)
        .mapValues(
          perItr => perItr.foldLeft(0.0) {
            (t, r) => t + r._3 / perItr.size
          }
        )
        .seq
  }

}
