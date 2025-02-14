package project

object FlightDataParser {
  def getInt(str:String) : Int = {
    if (str.forall(Character.isDigit))
      str.toInt
    else
      -1
  }

  def parse_flight(line: String): (Int, Int, Int, String, Int, String, Int, String, String, String, String, String) = {
    val parts = line.split(",")
    val year = getInt(parts(0))
    val month = getInt(parts(1))
    val day = getInt(parts(2))
    val dep_time = parts(4)
    val dep_delay = getInt(parts(15))
    val arr_time = parts(6)
    val arr_delay = getInt(parts(14))
    val carrier = parts(8)
    val flightnum = parts(9)
    val tailnum = parts(10)
    val origin = parts(16)
    val dest = parts(17)
    (year, month, day, dep_time, dep_delay, arr_time, arr_delay, carrier, tailnum, flightnum, origin, dest)
  }

  def parse_aircraft(line: String): (String, String, String, String, String, Int) = {
    val parts = line.split(",")
    if (parts.length != 9) {
      (parts(0), "", "", "", "", 0)
    } else {
      val tailnum: String = parts(0)
      val manufacturer = parts(2)
      val model = parts(4)
      val type_aircraft = parts(6)
      val engine_type = parts(7)
      val year = getInt(parts(8))
      (tailnum, manufacturer, model, type_aircraft, engine_type, year)
    }
  }

  def parse_carrier(line: String) = {
    val parts = line.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
    val code = parts(0).toUpperCase
    val description = parts(1)
    (code, description)
  }

  def parse_airport(line: String) = {
      val parts = line.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val iata = parts(0).toUpperCase
      val airport = parts(1)
      val city = parts(2)
      val state = parts(3)
      val country = parts(4)
      val lat = parts(5)
      val long = parts(6)
      (iata, airport, city, state, country, lat, long)
  }
}
