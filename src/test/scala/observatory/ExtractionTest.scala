package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import utils.SparkJob

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite with SparkJob {

  val year = 1975
  val debug = true

  val stationsPath: String = "/stations.csv"
  val temperaturePath: String = s"/$year.csv"

}