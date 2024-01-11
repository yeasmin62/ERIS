import scala.collection.mutable.ListBuffer
object GuiMain {

  val p = new RAParser()
  
  def getQuery(connector: Connector, q: Absyn.Query): Database.Rel = {
    val q_sql = Absyn.Query.sql(q)
    val conn = connector.getConnection()
    val ps = conn.prepareStatement(q_sql)
    val stream = Database.streamQuery(ps)
    Database.getRelation(stream, q.schema)
  }

/// to pro the queries or equations
  def queryprint(
      connector: Connector,
      specfile: String,
      boundlist: ListBuffer[Double],
      ctx: Database.InstanceSchema,
      enc:String,
      encoding: Encoding
  ): (Database.Schema, Absyn.Query,Absyn.Query,Database.Schema,Database.Schema,String,String, Database.Rel,Database.Rel, List[Database.Equation]) = {
    val str = specfile
    var q: Absyn.Query = null
    var q0:Absyn.Query = null
    var q0vc:Absyn.Query = null
    var q01: Map[String,Absyn.Query] = null
    var q0vc1:Map[String,Absyn.Query]= null
    var enc_schema:Map[String,Database.Schema] = null
    // if (str.contains("spec")) {
    //   q = SolveView.view(connector, str, encoding, Map("#" + 1-> "20200101D", "#" + 2-> "20200101"), false, false)
    // } else {
    //   q = SolveView.view1(connector, str, encoding, Map())
    // }
    q = SolveView.view1(connector, str, encoding, Map())
    val schema = Absyn.Query.tc(ctx, q)

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

    
    
    val schema0 = Absyn.Query.tc(enc_schema, q0)
    val schema0vc = Absyn.Query.tc(enc_schema, q0vc)
    val sql0 = Absyn.Query.sql(q0)
    val sql0vc = Absyn.Query.sql(q0vc)
    val result0 = getQuery(connector, q0)
    val result0vc = getQuery(connector, q0vc)
    val es = encoding.instanceSchemaEncoding(ctx)
    val conn = connector.getConnection()
    val eq = SolveView.equation(conn, encoding, ctx, es, q)
    

    (schema, q0, q0vc,schema0,schema0vc, sql0, sql0vc, result0, result0vc, eq)
  }
  
// to print the results of cost functions which is also called errors
  def costprint(
      connector: Connector,
      specfile: String,
      boundlist: ListBuffer[Double],
      ctx: Database.InstanceSchema,
      encoding: Encoding,
      flag_error:String,
      flag_null:Boolean
  ): Any = {
    val str = specfile
    val (valuation, objective, eqs, vars, eqCreationTime, solveTime) =
      SolveView.view(connector, str, boundlist, encoding, Map("#" + 1-> "20200101", "#" + 2-> "20200101"), flag_error, flag_null)
    (valuation, objective, eqs, vars, eqCreationTime, solveTime)
  }

}
