object MainPartitioning {

  val p = new RAParser()

  def main(args:Array[String]) : Unit = {
    val connector = Connector(args(0), args(1), args(2), args(3))
   var input_text = """
// t1:=(climate_temperature(date = '20200101D'));
// t2:=(t1[latitude, longitude SUM sea_surface_temperature]);
// t3:=(copernicus_temperature(date = '20200101'));
// t4:=(t3[latitude, longitude SUM sea_surface_temperature]);
// t5:=(t2 DUNION[src] t4)[COAL src]




// t1:=((climate_temperature{latitude->degree005} JOIN resolution005){degree020->latitude});
// t2:=(t1[latitude SUM sea_surface_temperature]);
// t4:=(((copernicus_temperature{latitude->degree005}) JOIN resolution005){degree020->latitude});
// t5:=(t4[latitude SUM sea_surface_temperature]);
// t6:=(t2 DUNION[src] t5)[COAL src]



t1:=((climate_temperature{latitude->degree005} JOIN resolution005));
t2:=(t1[degree020 SUM sea_surface_temperature]);
t4:=(((copernicus_temperature{latitude->degree005}) JOIN resolution005));
t5:=(t4[degree020 SUM sea_surface_temperature]);
t7:=(t2 DUNION[src] t5)[COAL src]




// t4:=(t1{latitude->degree005});
// t6:=(t4 JOIN resolution005{degree020->la}) JOIN resolution005{degree005->longitude};
// t8:=((t6{counter:=1})[la, degree020 SUM sea_surface_temperature, counter]);
// t9:=(t8{avg_sst:= sea_surface_temperature/counter})[avg_sst];
// t10:=(copernicus_temperature(date = '20200101'));
// t11:=(t10 JOIN resolution005{degree020->la}) JOIN resolution005{degree005->longitude};
// t15:=((t11{counter:=1})[la, degree020 SUM sea_surface_temperature, counter]);
// t16:=(t15{avg_sst:=sea_surface_temperature/counter})[avg_sst];
// t17:=(t9 DUNION[src] t16)[COAL src]







  // (((((climate_temperature(date = '20200101D')){counter:=1})[latitude SUM sea_surface_temperature,counter]){avg_sst:=sea_surface_temperature/counter})[avg_sst] DUNION[src] ((((copernicus_temperature(date = '20200101')){counter:=1})[latitude SUM sea_surface_temperature,counter]){avg_sst:=sea_surface_temperature/counter})[avg_sst])[COAL src]    
         """
    val conn = connector.getConnection()


    val ctx = Database.loadSchema(conn)
    var schema1: String = " "
    var enc=args(4)
    val encoding = Encoding.encoder_to_use(args.applyOrElse(4, { _: Int => "nf2_sparsev"}))

    // should only be called after queries are typechecked
    def getQuery( q: Absyn.Query): Database.Rel = {
      val q_sql = Absyn.Query.sql(q)
      val ps = conn.prepareStatement(q_sql)
      val stream = Database.streamQuery(ps)
      Database.getRelation(stream, q.schema)
    }

      print("query> ")
      val str = input_text
      try {
        val q = SolveView.view1(connector, str, encoding, Map())
        val schema = Absyn.Query.tc(ctx,q)
        println("----->>>>> Result schema")
        // println(schema.toString)
        val (q0,qm,(q0vc,qmvc)) = EncodePartitioning.queryEncoding(q)
        println("----->>>> Query encoding")
        println(q0)
        println(qm)
        println(q0vc)
        println(qmvc)
        val enc_schema = EncodePartitioning.instanceSchemaEncoding(ctx)
        println(enc_schema)
        val schema0 = Absyn.Query.tc(enc_schema,q0)
        val schemam = qm.map{case(f,qf) => (f,Absyn.Query.tc(enc_schema,qf))} 
        val schema0vc = Absyn.Query.tc(enc_schema,q0vc)
        val schemamvc = qmvc.map{case(f,qf) => (f,Absyn.Query.tc(enc_schema,qf))} 
        println("----->>>>>>> Query encoding schema")
        println(schema0)
        println(schemam)
        println(schema0vc)
        println(schemamvc)
        val sql0 = Absyn.Query.sql(q0)
        val sqlm = qm.map{case(f,qf) => (f,Absyn.Query.sql(qf))}
        val sql0vc = Absyn.Query.sql(q0vc)
        val sqlmvc = qmvc.map{case(f,qf) => (f,Absyn.Query.sql(qf))}
//        val sql0vc = Absyn.Query.sql(q0vc)
        println("----->>>>>>> Query encoding SQL")
        // println(sql0)
        // println(sqlm)
        // println("========")
        println("Base result:")
        val result0 = getQuery(q0)
        // println(result0)
        println("========")
        println("Field results:")
        qm.map{case(f,qf) => println("-------- "+f);println(getQuery(qf))}
        println("========")
        println("========")
        println("Base result VC:")
        val result0vc = getQuery(q0vc)
        println(result0vc)
        println("========")
        println("Field results VC:")
        qmvc.map{case(f,qf) => println("-------- "+f);println(getQuery(qf))}
        println("========")
        val (valuation,objective,eqs,vars,eqCreationTime,solveTime) = VirtualSolver1.solve1(connector, q, encoding, false)
        println(s";$eqs;$vars;"+eqCreationTime+";"+solveTime+";"+objective)
      } catch {
        case Absyn.TypeError(msg) => println("Type error: " + msg)
      }
  }
}