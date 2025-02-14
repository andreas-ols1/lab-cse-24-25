package project

import org.apache.spark.sql.SparkSession
import utils._
import FlightDataParser._

object CarrierAirportDelayJobOptimized {

    val flightsPath = "datasets/project/flights.csv"
    val airportsPath = "datasets/project/airports.csv"
    val carriersPath = "datasets/project/carriers.csv"
    val outputDir = "output_optimized/"

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("Carrier aiport delay job optimized").getOrCreate()

        if(args.length == 0)    {
            println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
            return
        }
    
        val deploymentMode = args(0)

        val rddFlights = spark.sparkContext.textFile(Commons.getDatasetPath(deploymentMode, flightsPath)).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(FlightDataParser.parse_flight)
        val rddAirports = spark.sparkContext.textFile(Commons.getDatasetPath(deploymentMode, airportsPath)).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(FlightDataParser.parse_airport)
        val rddCarriers = spark.sparkContext.textFile(Commons.getDatasetPath(deploymentMode, carriersPath)).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(FlightDataParser.parse_carrier)

        val carrierAirportDelays = rddFlights
            .map { case (_, _, _, _, _, _, arr_delay, carrier, _, _, origin, _) => 
                ((carrier, origin), (arr_delay, 1)) 
            }
            .coalesce(1)

        val avgDelayPerAirportCarrier = carrierAirportDelays
            .reduceByKey { case ((sumDelay1, count1), (sumDelay2, count2)) =>
                (sumDelay1 + sumDelay2, count1 + count2)
            }
            .mapValues { case (totalDelay, count) => totalDelay.toDouble / count }


        val worstAirportForEachCarrier = avgDelayPerAirportCarrier
            .map { case ((carrier, airport), avgDelay) => (carrier, (airport, avgDelay)) }
            .reduceByKey { case ((airport1, delay1), (airport2, delay2)) =>
                if (delay1 > delay2) (airport1, delay1) else (airport2, delay2)
            }

        val airportMap = rddAirports
            .map(x => (x._1.trim.toUpperCase, x._2.trim))
            .collectAsMap()
        val carrierMap = rddCarriers
            .map(x => (x._1.trim.toUpperCase, x._2.trim))
            .collectAsMap()

        val airportBroadcast = spark.sparkContext.broadcast(airportMap)
        val carrierBroadcast = spark.sparkContext.broadcast(carrierMap)

        val results = worstAirportForEachCarrier.map { case (carrier, (airport, avgDelay)) =>
            val airportName = airportBroadcast.value.getOrElse(airport.trim.toUpperCase, "Unknown")
            val carrierName = carrierBroadcast.value.getOrElse(carrier.trim.toUpperCase, "Unknown")
            (s"$carrier ($carrierName)", s"$airport ($airportName)", avgDelay)
        }

        results.saveAsTextFile(Commons.getDatasetPath(deploymentMode, outputDir))
    }
}