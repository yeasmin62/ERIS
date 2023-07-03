object COVIDDeaths1 {

  def main(args:Array[String]) : Unit = {
    Class.forName("org.postgresql.Driver")

    val hostname = args(0)
    val dbname = args(1)
    val username = args(2)
    val password = args(3)
    val specfile = args(4)
    val encoding = Encoding.encoder_to_use(args.applyOrElse(5,{_:Int =>"nf2_sparsev"}))
    val flagV = args(6).toBoolean
    val flag_null = args(7).toBoolean
    val flag_error = args(8)

    val connector = Connector(hostname, dbname, username, password)
    //var newfile = newview(specfile)

    //./run-loader.sh $1 $2 $3 $4 r2 $5
    Loader.load(connector,"countries", encoding, false,flagV)
    Loader.load(connector,"dates", encoding, false,flagV)
    Loader.load(connector,"firstadminlevels", encoding, false,flagV)
    Loader.load(connector,"weeks", encoding, false,flagV)
//    Loader.load(connector,"coviddata_percountry", encoding, false)
    Loader.load(connector,"coviddata_perregion", encoding, false,flagV)
    Loader.load(connector,"eurostats_percountry_perweek", encoding, false,flagV)
    Loader.load(connector,"eurostats_percountry_perweek_estimated", encoding, false,flagV)
//    Loader.load(connector,"eurostats_percountry_peryear", encoding, false)
    Loader.load(connector,"eurostats_perregion", encoding, false,flagV)
    Loader.load(connector,"eurostats_perregion_estimated", encoding, false,flagV)
    Loader.load(connector,"jhu_percountry", encoding, false,flagV)
//    Loader.load(connector,"jhu_perregion", encoding, false)

    val conn = connector.getConnection()
    // Because of Brexit, Eurostats only provides data of UK until 2020-W51
    val countries = List("NL")//,"SE")//, "DE","IT","ES","UK") //"RU","FR","NO", "DK",
    var querylist: List[Absyn.Query] = List()
    var vlist: Map[String, String] = Map()
    //var newline = newview(specfile)
    //println("Linesize" + linesize)

    println("KindOfQuery;Shift;Country;Week;#Eq;#Vars;Eq. creation time;Solve time;Average squared error")

    for (shift <- 1 to 2) {
      countries.map{co =>
         // There is no estimation of deaths, because W53 is quite exceptional and it does not exists all years (thus, this is avoided)
        val timequery = " select w.id, min(dpast.id) as paststart, max(dpast.id) as pastend, min(dcurrent.id) as currentstart, max(dcurrent.id) as currentend from weeks w join weeks past on w.weekminus"+shift+" = past.id join dates dcurrent on w.id=dcurrent.week join dates dpast on past.id=dpast.week where w.id>='2020W05' and w.id<='2021W06' and w.id<>'2020W53' group by w.id order by w.id;"

      val preparedStatement = conn.prepareStatement(timequery)
      val res = List() ++ Database.streamQuery(preparedStatement)
      res.map{row =>
        val wid = row("id").get
        val ps = row("paststart").get
        val pe = row("pastend").get
        val cs = row("currentstart").get
        val ce = row("currentend").get
        for(i<-1 to 1)
          {
            vlist = Map("#" + i-> co, "#" + (i+1)-> wid)
          }


        //println("newfile " + newfile1)
//
//        newspec = newline.replace("#", co)
//        newspec = newspec.replace("$", wid)
//        val flag = true
        //println("NewSpec" + newspec)
        if (shift==1) {
          // Coalescing all data from both EUROStats and JHU as in the paper diagram

          val (_,objective,eqs,vars,eqCreationTime,solveTime) = SolveView.view(connector, specfile, encoding, vlist, flag_error, flag_null)
          // println(s"WithoutJHURegions;-;$co;$wid;$eqs;$vars;"+eqCreationTime+";"+solveTime+";"+objective)
          }
        else {
          // Coalescing all data from both EUROStats and JHU as in the paper diagram
          val (_, objective, eqs, vars, eqCreationTime, solveTime) = SolveView.view(connector, specfile, encoding, vlist,flag_error,flag_null)
          // println(s"WithJHURegions;$shift;$co;$wid;$eqs;$vars;" + eqCreationTime + ";" + solveTime + ";" + objective)
        }
        }
      }
    }

    // cleanup
    Loader.load(connector,"countries", encoding, true,flagV)
    Loader.load(connector,"dates", encoding, true,flagV)
    Loader.load(connector,"firstadminlevels", encoding, true,flagV)
    Loader.load(connector,"weeks", encoding, true,flagV)
//--    Loader.load(connector,"coviddata_percountry", encoding, true)
    Loader.load(connector,"coviddata_perregion", encoding, true,flagV)
    Loader.load(connector,"eurostats_percountry_perweek", encoding, true,flagV)
    Loader.load(connector,"eurostats_percountry_perweek_estimated", encoding, true,flagV)
//--    Loader.load(connector,"eurostats_percountry_peryear", encoding, true)
    Loader.load(connector,"eurostats_perregion", encoding, true,flagV)
    Loader.load(connector,"eurostats_perregion_estimated", encoding, true,flagV)
    Loader.load(connector,"jhu_percountry", encoding, true,flagV)
//--    Loader.load(connector,"jhu_perregion", encoding, true)
    conn.close()
  }

//converts the specfile into lines
//

}




