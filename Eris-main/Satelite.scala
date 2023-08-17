
object Satelite {

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")

    val hostname = args(0)
    val dbname = args(1)
    val username = args(2)
    val password = args(3)
    val encoding = Encoding.encoder_to_use(args.applyOrElse(4, { _: Int => "partitioning" }))
    val flagV = args(5).toBoolean

    val connector = Connector(hostname, dbname, username, password)
//    var flag_error = args(6).toBoolean
//    var flag_null = args(7).toBoolean

    Loader.load(connector, "copernicus_temperature", encoding, false, flagV)
    Loader.load(connector, "dates", encoding, false, flagV)
    // Loader.load(connector, "copernicus_chlorophyll", encoding, false, flagV)
    Loader.load(connector, "months", encoding, false, flagV)
    Loader.load(connector, "climate_temperature", encoding, false, flagV)
    // Loader.load(connector, "climate_chlorophyll", encoding, false, flagV)
    Loader.load(connector, "pathfinder_temperature", encoding, false, flagV)
    Loader.load(connector, "modisaqua_temperature", encoding, false, flagV)
    // Loader.load(connector, "modisaqua_chlorophyll", encoding, false,flagV)
    Loader.load(connector, "resolution001", encoding, false, flagV)
    Loader.load(connector, "resolution004", encoding, false, flagV)
    Loader.load(connector, "resolution005", encoding, false, flagV)
    Loader.load(connector, "resolution020", encoding, false, flagV)
    Loader.load(connector, "semiday", encoding, false, flagV)
  }
}