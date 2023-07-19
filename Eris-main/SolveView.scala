//import Absyn._


import scala.collection.immutable.List
import scala.io.Source

// Evaluates multiple  query and combines it in single query.


object  SolveView {


  def handleSpecification(connector:Connector, ctx: Database.InstanceSchema, spec: List[(String,Absyn.Query)]) = {
    var vt: List[String] = List()
    var vq: List[Absyn.Query] = List()
    var spec1: List[(Absyn.Query)] = List()
    //var spec2: List[(String,Absyn.Query)]= List()

      spec.foldLeft(ctx) { case (ctx, (vtable, vquery)) =>
        val res: Absyn.Query = combineQuery.queryRet(vq, vt, vquery)
        vt=vt:+vtable
        vq=vq:+res
        spec1 = List(res)
        //spec2 = List((vtable, res))
        ctx
      }
    spec1
  }

//take inputs from specfile, parse the file into query, combines all the query into single one, sends that to VirtualSolver to solve the equations
  def view(connector: Connector, specfile: String, encoding: Encoding, vlist:Map[String,String], flag_error:String, flag_null:Boolean):(List[(String,Double)],Double,Int,Int,Double,Double) = {
    //println("\n specfile \n")

    val p = new RAParser() //creates an object of RAParser
    var spec: List[(String,Absyn.Query)] = List()
    //println(" line number " + line)
    //flag is used to differentiate between the inputs from covid file and other file
    spec = p.parse(p.specification, specfile, vlist)
    //println("Spec " + spec)
    var spec1: List[(Absyn.Query)] = List()
    var result:(List[(String,Double)],Double,Int,Int,Double,Double) = (List(),0,0,0,0,0)

    val conn = connector.getConnection()
    val ctx = Database.loadSchema(conn)
    //println("Spec " + spec)


    try {
      spec1 = handleSpecification(connector, ctx, spec)
      //print(spec1)

    } catch {
          case Absyn.TypeError(msg) => println("Type error: " + msg)

    }
    //println("After combination Spec " + spec1.length)
    var cstring = spec1(0)
    //println("After combonation Spec " + cstring)

    result = flag_error match {
      case "ASE" => {VirtualSolver.solve1(connector, cstring, encoding, flag_null)}
      case "AAE" => {VirtualSolverAAE.solve1(connector, cstring, encoding, flag_null)}
      case "Value_Interval" => {VirtualSolver1.solve1(connector, cstring, encoding, flag_null)}

    }

    // if(flag_error)
    //   {
    //     result = VirtualSolver.solve1(connector, cstring, encoding, flag_null)
    //   }
    // else
    //   {
    //     result = VirtualSolverAAE.solve1(connector, cstring, encoding, flag_null)
    //   }

    val (valuation,objective,eqs,vars,eqCreationTime,solveTime) = result
    println(s";$eqs;$vars;"+eqCreationTime+";"+solveTime+";"+objective)
    (valuation,objective,eqs,vars,eqCreationTime,solveTime)
  }

  def view1(connector: Connector, specfile: String, encoding: Encoding, vlist:Map[String,String]):Absyn.Query = {
    //println(specfile)

    //var lines = try specfile.mkString finally specfile.close()

    val p = new RAParser() //creates an object of RAParser
    var spec: List[(String, Absyn.Query)] = List()
    //println(" line number " + line)
    //flag is used to differentiate between the inputs from covid file and other file
    if (specfile.contains("spec"))
    {
      spec = p.parse(p.specification, specfile, vlist)
    } else {
      spec = p.parseStr(p.specification, specfile)
    }
    //println("Spec " + spec)
    var spec1: List[(Absyn.Query)] = List()
    var result: (List[(String, Double)], Double, Int, Int, Double, Double) = (List(), 0, 0, 0, 0, 0)

    val conn = connector.getConnection()
    val ctx = Database.loadSchema(conn)
    //println("Spec " + spec)


    try {
      spec1 = handleSpecification(connector, ctx, spec)

    } catch {
      case Absyn.TypeError(msg) => println("Type error: " + msg)

    }
    //println("After combination Spec " + spec1.length)
    var cstring = spec1(0)

    cstring
  }
  // def equList(equation:List[Database.Equation]) : Unit= {
  //   equation match {
  //     case Nil => Absyn.NYI
  //     case head :: tail =>
  //       // print("\n")
  //       // print(head)
  //       equList(tail)
  //   }
  // }

   def equation(conn: java.sql.Connection, encoder: Encoding, ctx: Database.InstanceSchema, es: Database.InstanceSchema, str: Absyn.Query) =
  {
    // val q = p.parseStr(p.query,str)
    val schema = Absyn.Query.tc(ctx,str)
    val eq = encoder.queryEncoding(str)
    val equa = encoder.iterateEncodedConstraints(conn,eq,es)
    val equaList = equa.toList
    // print(equaList)
    equaList
    // val (valuation,objective) = solveList(equations.toList)
    // (valuation, objective)
  }
        
    
      
    
    

  def main(args:Array[String]) : Unit = {
    Class.forName("org.postgresql.Driver")

    val hostname = args(0)
    val dbname = args(1)
    val username = args(2)
    val password = args(3)
    val interactive = args.length < 5
    val flag_null = args(6)
    val flag_error = args(7)

    val connector = Connector(hostname, dbname, username, password)
    val encoding = Encoding.encoder_to_use(args.applyOrElse(5,{_:Int =>"nf2_sparsev"}))



    if (interactive) {
      // REPL loop
      val conn = connector.getConnection()
      val p = new RAParser()

      while (true) {
        val ctx = Database.loadSchema(conn)

        print("view> ")
        val str = scala.io.StdIn.readLine()
        val (vtable,vquery) = p.parseStr(p.viewDef,str)

        // check whether table is alread in schema, if so ask
        if (ctx.contains(vtable)) {
          println("Warning: table " + vtable + " is already present in schema, delete it?")
          val ans = scala.io.StdIn.readLine()
          if(ans != "y") {
            System.exit(0)
          }
        }

        try {
          VirtualSolver1.solve1(connector, vquery, encoding,flag_null.toBoolean)
        } catch {
          case Absyn.TypeError(msg) => println("Type error: " + msg)
        }
      }
    } else { // Read from spec file
      var vlist:Map[String, String] = Map()
      // val flag_error = false
      // val flag_null = false
      var date = List("20200101","20200102","20200103","20200104","20200105","20200106","20200107","20200108","20200109","20200110","20200111","20200112","20200113","20200114","20200115","20200116","20200117","20200118","20200119", "20200120", "20200121","20200122","20200123","20200124","20200125","20200126","20200127","20200128","20200129","20200130","20200131")
    //  var date = List("20200101", "20200102","20200103")
      for(d<-date)
      {
        // print(d)
//        print("\n")
        val c = d.substring(0, d.length()-1)
        print(d)
        vlist = Map("#" + 1-> d, "#" + 2-> d)
//        vlist = Map("#" + 1-> d)
        view(connector, args(4), encoding, vlist, flag_error, flag_null.toBoolean)
        
      }

//      view(connector, args(4), encoding, vlist, flag_error, flag_null)

    }

  }
}