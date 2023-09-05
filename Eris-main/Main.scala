
object Main {

  val p = new RAParser()

  def main(args:Array[String]) : Unit = {
    val connector = Connector(args(0), args(1), args(2), args(3))
   var input_text = """
// t1:=(climate_temperature(date = '20200101D'));
// t2:=((t1{counter:=1})[latitude,longitude SUM sea_surface_temperature,counter]);
// t3:=(t2{avg_sst:=sea_surface_temperature/counter})[avg_sst];
// t4:=(copernicus_temperature(date = '20200101'));
// t5:=((t4{counter:=1})[latitude,longitude SUM sea_surface_temperature,counter]);
// t6:=(t5{avg_sst:=sea_surface_temperature/counter})[avg_sst];
// t7:= (t3 DUNION[src] t6)[COAL src]

t1:=(climate_temperature);
t2:=(t1 JOIN semiday{id->date});
t3:=(t2(dateofsemiday = '20200101'));
t4:=((t3{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t5:=((t4{sst:=sea_surface_temperature/counter}){latitude->degree005});
t6:=((t5 JOIN resolution005){degree005->la});
t7:=(((t6{degree020->latitude}){longitude->degree005}) JOIN resolution005{degree020->longitude});
t8:=((t7{counter1:=1})[latitude,longitude SUM sst, counter1]);
t9:=(t8{avg_sst:= sst/counter1})[avg_sst];
t11:=(modisaqua_temperature(date = '20200101'));
t12:=(t11{sst:=sea_surface_temperature+273.15});
t13:=((t12{latitude->degree004}) JOIN resolution004);
t14:=((t13{degree004->la}){degree020->latitude});
t15:=((t14{longitude->degree004}) JOIN (resolution004{degree020->longitude}));
t16:=((t15{counter:=1})[latitude,longitude SUM sst, counter]);
t17:=(t16{avg_sst:=sst/counter})[avg_sst];
t18:=(t9 DUNION[src] t17)[COAL src]


      
         """
    val conn = connector.getConnection()


    val ctx = Database.loadSchema(conn)
    var schema1: String = " "
    var enc=args(4)
    val encoding = Encoding.encoder_to_use(args.applyOrElse(4, { _: Int => "nf2_sparsev"}))

    //println(ctx)

    // should only be called after queries are typechecked
    def getQuery(q: Absyn.Query): Database.Rel = {
      val q_sql = Absyn.Query.sql(q)
      val ps = conn.prepareStatement(q_sql)
      val stream = Database.streamQuery(ps)
      Database.getRelation(stream, q.schema)
    }


      print("query> ")
      val str = input_text
      try {
        var q0:Absyn.Query = null
        var q0vc:Absyn.Query = null
        var q01: Map[String,Absyn.Query] = null
        var q0vc1:Map[String,Absyn.Query]= null
        var enc_schema:Map[String,Database.Schema] = null
        val q = SolveView.view1(connector, str, encoding, Map())
        val schema = Absyn.Query.tc(ctx, q)
        schema1 = schema.toString
        // println("----->>>>> Result schema")
        // println(schema.toString)
        if (enc.contains("nf2_sparsev"))
        {
          // print(enc)
          val (iq0,iq0vc) = EncodeNF2_SparseV.queryEncoding(q)
          q0 = iq0
          q0vc = iq0vc
          enc_schema = EncodeNF2_SparseV.instanceSchemaEncoding(ctx)
          // print(q0)


        }
        else {
          // print("no")
          val (iq0,q01, (iq0vc,q0vc1)) = EncodePartitioning.queryEncoding(q)
          q0 = iq0
          q0vc = iq0vc
          enc_schema = EncodePartitioning.instanceSchemaEncoding(ctx)
          // print(q0)
        }
        
        println("----->>>> Query encoding")
        // println(q0)
        // println(q0vc)
        // println(enc_schema)
        val schema0 = Absyn.Query.tc(enc_schema, q0)
        val schema0vc = Absyn.Query.tc(enc_schema, q0vc)
        println("----->>>>>>> Query encoding schema")
        // println(schema0)
        // println(schema0vc)
        val sql0 = Absyn.Query.sql(q0)
        val sql0vc = Absyn.Query.sql(q0vc)
        println("----->>>>>>> Query encoding SQL")
        // println(sql0)
        // println(sql0vc)
        println("========")
        println("Base result:")
        val result0 = getQuery(q0)
        // println(result0)
        println("========")
        println("========")
        println("Base result VC:")
        val result0vc = getQuery(q0vc)
        // println(result0vc)
        println("========")
        val (valuation,objective,eqs,vars,eqCreationTime,solveTime) = VirtualSolver1.solve1(connector, q, encoding, false)
        println(s";$eqs;$vars;"+eqCreationTime+";"+solveTime+";"+objective)
      } catch {
        case Absyn.TypeError(msg) => println("Type error: " + msg)
      }

    }



}
