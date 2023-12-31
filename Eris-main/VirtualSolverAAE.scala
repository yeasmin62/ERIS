import scala.collection.immutable._
import java.lang.ProcessBuilder
import java.io.File
import java.io.BufferedReader
import java.io.InputStream
import java.io.FileWriter
import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import play.api.libs.json.Json

object VirtualSolverAAE {
  val p = new RAParser()

  type Emitter[A] = PrintWriter => A

  import Timer.timeIt

  def toLPForm(vars: List[String], sys: List[(Database.Vector[String],Double)]): (List[List[Double]], List[Double]) = {
    val A = sys.map{v => vars.map{x => v._1.getOrElse(x,0.0)}}
    println("Virtual Solver ToLpform A " + A)
    val b = sys.map(_._2)
    println("Virtual Solver ToLpform b " + b)
    (A,b)
  }

  def toLPFormSparse(sys: List[(Database.Vector[String],Double)]): (List[Database.Vector[String]], List[Double]) = {
    val A = sys.map(_._1)
    println("Virtual Solver ToLpformSparse A " + A)
    val b = sys.map(_._2)
    println("Virtual Solver ToLpformSparse b " + b)
    (A,b)
  }

  // building up lists by string concatenation is inefficient, we should be emitting straight to output stream
  def toPythonList(l: List[Double]): String = {
    "["+l.map(_.toString).mkString(",")+"]"
  }

  def emitPythonList(l: List[Double]): Emitter[Unit] = {
    @tailrec
    def emit(l: List[Double], wr: PrintWriter): Unit = l match {
      case Nil => ()
      case dbl::Nil =>
        println("VirtualSolver emitPythonList dbl " + dbl)
        wr.write(dbl.toString)
      case dbl::dbls =>
        println("VirtualSolver emitPythonList dbl " + dbl)
        wr.write(dbl.toString)
        wr.write(",")
        emit(dbls,wr)
    }
    {wr =>
      wr.write("[")
      emit(l,wr)
      wr.write("]")
    }

  }

  def emitPythonListIter(l: Iterator[Double]): Emitter[Unit] = {
    @tailrec
    def emit(wr: PrintWriter): Unit =
      if (l.hasNext) {
        val dbl = l.next
        println("VirtualSolver emitPythonListIter dbl " + dbl)
        wr.write(dbl.toString)
        if (l.hasNext) {
          wr.write(",")
          emit(wr)
        }
      }
    {wr =>
      wr.write("[")
      emit(wr)
      wr.write("]")
    }
  }

  def toPythonMatx(a: List[List[Double]]): String = {
    "["+a.map(toPythonList).mkString(",")+"]"
  }

  def emitPythonMatx(a: List[List[Double]]): Emitter[Unit] = {
    @tailrec
    def emit(l: List[List[Double]],wr: PrintWriter): Unit = l match {
      case Nil => ()
      case vec::Nil => emitPythonList(vec)(wr)
      case vec::vecs =>
        println("VirtualSolver emitPythonMatx vec" + vec)
        emitPythonList(vec)(wr)
        wr.write(",")
        emit(vecs,wr)
    }
    {wr =>
      wr.write("[")
      emit(a,wr)
      wr.write("]")
    }

  }


  def sparseMatx(fvs: List[String], a: List[Database.Vector[String]]) = {
    val m = a.length
    val atbl = List.tabulate(m)(identity).zip(a)
    val n = fvs.length
    val fvtbl = fvs.zip(List.tabulate(n)(identity)).toMap
    val coords:List[(Int,Int,Double)] = atbl.flatMap{case (row: Int,vec: Database.Vector[String]) =>
      vec.toList.map{case (vr,vl) =>
        println("VirtualSolver sparseMatx (row, fvtbl(vr), vl)" + (row, fvtbl(vr), vl))
        (row, fvtbl(vr), vl)
      }
    }
    val i = toPythonList(coords.map(_._1))
    val j = toPythonList(coords.map(_._2))
    val data = toPythonList(coords.map(_._3))
    println("VirtualSolver sparseMatx (data,i,j,m,n)" + (data,i,j,m,n))
    (data,i,j,m,n)
  }

  def toPythonMatxSparse(fvs: List[String], a: List[Database.Vector[String]]): String = {
    val (data,i,j,m,n) = sparseMatx(fvs,a)
    println("VirtualSolver toPythonMatxSparse (data,i,j,m,n)" + (data,i,j,m,n))
    s"sparse.coo_matrix(($data,($i,$j)),shape=($m,$n))"
  }
 // This part is to get the data from data_b
  def toPythonMatxSparse_b(fvs: List[String], a: List[Database.Vector[String]]): String = {
    val (data,i,j,m,n) = sparseMatx(fvs,a)
    val s:Int = 1
    //println("VirtualSolver toPythonMatxSparse (data,i,j,m,n)" + (data,i,j,m,n))
    s"sparse.coo_matrix(($data,($i,$j)),shape=($m,$s))"
  }

  def emitPythonMatxSparse(fvs: List[String], a: List[Database.Vector[String]]): Emitter[Unit] = {
    val (data,i,j,m,n) = sparseMatx(fvs,a)
    val emitter: Emitter[Unit] = {wr =>
      wr.write("sparse.coo_matrix((")
      wr.write(data)
      wr.write(",(")
      wr.write(i)
      wr.write(",")
      wr.write(j)
      wr.write(s")),shape=($m,$n))")
    }
    //println(" VirtualSOlver emitPythonMatxSparse emitter" + emitter)
    emitter
  }
  // This part is to get the data from data_b
  def emitPythonMatxSparse_b(fvs: List[String], a: List[Database.Vector[String]]): Emitter[Unit] = {
    val (data,i,j,m,n) = sparseMatx(fvs,a)
    val s:Int = 1
    val emitter: Emitter[Unit] = {wr =>
      wr.write("sparse.coo_matrix((")
      wr.write(data)
      wr.write(",(")
      wr.write(i)
      wr.write(",")
      wr.write(j)
      wr.write(s")),shape=($m,$s))")
    }
    //println(" VirtualSOlver emitPythonMatxSparse emitter" + emitter)
    emitter
  }


  def makeLP(n: Int, l: List[Double], a: List[List[Double]], u: List[Double]): String = {
    toOSQP(s"np.zeros(($n,$n))", s"np.zeros($n)",s"np.zeros($n)", toPythonList(l), toPythonMatx(a), toPythonList(u))
  }

  def emitLP(n: Int, a: List[List[Double]], c: List[Double]): Emitter[Unit] = {
    emitOSQPEq({wr => wr.write(s"np.zeros(($n,$n))")},
      {wr => wr.write(s"np.zeros($n)")},
      emitPythonMatx(a), // added this new term for b
      emitPythonMatx(a),
      emitPythonList(c))
  }

  def makeQP(fvs: List[String], l: List[Double], a: List[List[Double]], u: List[Double]): String = {
    val n = fvs.length
    // assign cost of x^2 to each variable not starting with "_"
    val diags = toPythonList(fvs.map{x => if (x(0) == '_') {0.0} else {1.0}})
    toOSQP(s"sparse.diags(np.zeros($n))", s"[$diags]",toPythonMatx(a), toPythonList(l), toPythonMatx(a), toPythonList(u))
  }

  def emitQP(fvs: List[String], a: List[List[Double]], c: List[Double]): Emitter[Unit] = {
    val n = fvs.length
    // assign cost of x^2 to each variable not starting with "_"
    val diags = emitPythonList(fvs.map{x => if (x(0) == '_') {0.0} else {1.0}})
    //println("Virtual SOlver emitQP diags " + diags)
    emitOSQPEq({wr =>
      wr.write(s"sparse.diags(np.zeros($n))")
    },
      {wr => wr.write(s"[")
        diags(wr)
        wr.write("]")},
      {wr =>
        wr.write(s"np.zeros($n)")  // added newly for b
      },
      emitPythonMatx(a),
      emitPythonList(c))
  }

  def makeQPSparse(fvs: List[String], l: List[Double], a: List[Database.Vector[String]], u: List[Double]): String = {
    val n = fvs.length
    // assign cost of x^2 to each variable not starting with "_"

    val diags = toPythonList(fvs.map{x => if (x(0) == '_') {0.0} else {1.0}})
    //println("Virtual SOlver makeQPSparse diags " + diags)
    toOSQP(s"sparse.diags(np.zeros($n))", s"[$diags]",toPythonMatxSparse_b(fvs,a), toPythonList(l), toPythonMatxSparse(fvs,a), toPythonList(u))
  }

  def emitQPSparse(fvs: List[String], a: List[Database.Vector[String]], c: List[Double]): Emitter[Unit] = {
    val n = fvs.length
    // assign cost of x^2 to each variable not starting with "_"
    val diags = emitPythonList(fvs.map{x => if (x(0) == '_') {0.0} else {1.0}})
    emitOSQPEq({wr =>
      wr.write(s"sparse.diags(np.zeros($n))")
      //println("VirtualSOlver emitQPSParse diags(wr)" + diags(wr))
      },
      {wr => wr.write(s"[")
        diags(wr)
        wr.write("]")},
      emitPythonMatxSparse_b(fvs,a),
      emitPythonMatxSparse(fvs,a),
      emitPythonList(c))
  }

  type BiMap[A,B] = (Map[A,B],Map[B,A])



  def emitData(r: Iterator[Database.Equation]): Emitter[(BiMap[String,Int],Int,Int)] = {wr =>
    @tailrec
    def emit(bimap: BiMap[String,Int],m: Int,n: Int, wr: PrintWriter): (BiMap[String,Int],Int,Int) = {
      if (r.hasNext) {
        val eqn = r.next
        // print(eqn)
        val vec = Database.Equation.toVec(eqn)
        //println("VirtualSOlver emitData eqn " + eqn + "vec " + vec)
        val (new_bimap,new_n) = vec.foldLeft(bimap,n){
          case ((bimap,n0),(None,c)) =>
            //wr.write(s"($c,$m,0),")
            //println("VirtualSOlver emitData emit ((bimap,n0),(None,c)) " + ((bimap,n0),(None,c)))
            (bimap,n0)
          case (((to,from),n0),(Some(x),c)) =>
            //println("VirtualSOlver emitData emit (((to,from),n0),(Some(x),c)) " + (((to,from),n0),(Some(x),c)))
            if (!to.contains(x)) {
              wr.write(s"($c,$m,$n0),")
              // extend maps and increase n
              ((to + (x -> n0), from + (n0 -> x)),n0+1)
            } else {
              val j = to(x)
              wr.write(s"($c,$m,$j),")
              ((to,from),n0)
            }
        }
        emit(new_bimap,m+1,new_n,wr)

      } else {
        (bimap,m,n)
      }
    }
    wr.write("np.array([(1.0,0,0),")
    val result = emit((Map("_one_" -> 0),Map(0 -> "_one_")),1,1,wr)
    wr.write("])\n")
    //println(result)
    result
  }

  def emitData_abs(z: Iterator[Database.Equation]): Emitter[(BiMap[String,Int],Int,Int)] = {wr =>
    @tailrec
    def emit(bimap: BiMap[String,Int],m: Int,n: Int, wr: PrintWriter): (BiMap[String,Int],Int,Int) = {
      var i:Int = 1
      if (z.hasNext) {
        val eqn = z.next
        // print(eqn + "\n")
        val vec = Database.Equation.toVec(eqn)
        //println("VirtualSOlver emitData vec \n" + vec + "\n\n")
        val (new_bimap,new_n) = vec.foldLeft(bimap,n){
          case ((bimap,n0),(None,c)) =>
            wr.write(s"($c,$m,0),")
            i = i+1
            //println("VirtualSOlver emitData emit" + n0 )
            (bimap,n0)
          case (((to,from),n0),(Some(x),c)) =>
            //println("VirtualSOlver emitData emit (((to,from),n0),(Some(x),c)) " + (((to,from),n0),(Some(x),c)))
            if (!to.contains(x)) {
              //wr.write(s"($c,$m,$n0),")
              //println(c , m, n0)
              // extend maps and increase n
              ((to + (x -> n0), from + (n0 -> x)),n0+1)
            } else {
              val j = to(x)
//              wr.write(s"($c,$m,$j),")
//              println(c , m, j)
              ((to,from),n0)
            }

        }
        emit(new_bimap,m+1,new_n,wr)

      } else {
        (bimap,m,n)
      }
    }
    wr.write("np.array([(1.0,0,0),")
    val result = emit((Map("_one_" -> 0),Map(0 -> "_one_")),1,1,wr)
    wr.write("])\n")
    //print(result)
    result
  }


  def emitWeights(n: Int,fvsmap: BiMap[String,Int]): Emitter[Unit] = {wr =>
    @tailrec
    def emit(n0: Int, wr: PrintWriter): Unit = {
      if (n0 < n) {
        wr.write((if (fvsmap._2(n0)(0) == '_') {"0.0"} else {"1.0"}) + ",")
        //println("emitWeights fvsmap " + fvsmap._2)
        //println("emitWeights fvsmap " + (fvsmap._2(n0)))
        emit(n0+1,wr)
      }
    }
    wr.write("sparse.diags([")
    emit(0,wr)
    wr.write("])\n")
  }

  def emitWeights1(n: Int,fvsmap: BiMap[String,Int]): Emitter[Unit] = {wr =>
    @tailrec
    def emit(n0: Int, wr: PrintWriter): Unit = {
      if (n0 < n) {
        wr.write((if (fvsmap._2(n0)(0) == '_') {"0.0"} else {"1.0"}) + ",")
        //println("emitWeights fvsmap " + fvsmap._2)
        //println("emitWeights fvsmap " + (fvsmap._2(n0)))
        emit(n0+1,wr)
      }
    }
    wr.write("[")
    emit(0,wr)
    wr.write("]\n")
  }
//// Emit weights in q for absolute error
  def emitWeights_abs(n: Int,fvsmap: BiMap[String,Int],flag_null:Boolean): Emitter[Unit] = {wr =>
    @tailrec
    def emit(n0: Int, wr: PrintWriter): Unit = {
      if (n0 < n) {
        if(flag_null)
          {
            wr.write((if (fvsmap._2(n0)(0) == '_') {"1.0"} else {"1.0"}) + ",") //if we want to consider NULLS
          }
        else
          {
            wr.write((if (fvsmap._2(n0)(0) == '_') {"0.0"} else {"1.0"}) + ",") //if we don't want to consider NULLs
          }
         //if null is not considered
        //println("emitWeights fvsmap " + fvsmap._2)
        //println("emitWeights fvsmap " + (fvsmap._2(n0)))
        emit(n0+1,wr)
      }
    }
    // to make the vaibales zero in the first positins
    def emit_zero(n0: Int, wr: PrintWriter):Unit= {
      if (n0 < n) {
        wr.write("0.0" + ",")
        emit_zero(n0 + 1, wr)
      }
    }

    wr.write("[")
      emit_zero(0,wr) // to add n number of zeros for n number of variables
      emit(0,wr)      // without considering null values
    wr.write("]\n")
  }



  // iteratively build OSQP input from stream of equations
  def emitQPSparseIter(r: Iterator[Database.Equation],flag_null:Boolean): Emitter[(BiMap[String,Int],Int,Int)] = {
    var (r1, r2) = r.duplicate
    wr =>
    wr.write(raw"""
import osqp
import numpy as np
from scipy import sparse
import math
import json

def get(x, i): 
    return np.array([r[i] for r in x])

""")
    wr.write("data = np.dtype('int64').type(")
    var (fvsmap,m,n) = emitData(r1)(wr)

    //print("m " + m + "n" + n)
    wr.write(")\n")
    // wr.write(s"P = sparse.csc_matrix((2*$n,2*$n))\n")

    //println("VirtualSolver emitQPSParseIter emitWeights(n,fvsmap)" + emitWeights(n,fvsmap))
    wr.write(s"q = np.dtype('int64').type(np.array(")
    emitWeights_abs(n,fvsmap,flag_null)(wr)
    wr.write(s"))\n")
    wr.write(s"data_b = np.dtype('int64').type(")
    var (fvsmap1,m1,n1) = emitData_abs(r2)(wr)
    wr.write(s")\n")
    wr.write(s"b = np.reshape(sparse.csc_matrix(sparse.coo_matrix((get(data_b,0),(get(data_b,1),get(data_b,2))),shape=($m,1))).toarray(),-1, order = 'F')\n")
    wr.write(s"A = sparse.vstack([sparse.vstack([sparse.hstack([sparse.csc_matrix(sparse.coo_matrix((get(data,0),(get(data,1),get(data,2))),shape=($m,$n))), sparse.csc_matrix(($m,$n))]), sparse.vstack([sparse.hstack([sparse.eye($n), -sparse.eye($n)]), sparse.hstack([-sparse.eye($n), -sparse.eye($n)])])], format='csc'),sparse.hstack([sparse.csc_matrix(($n,$n)),sparse.eye($n)])], format='csc')\n")
//    if(flag_null){
      wr.write(s"l = np.hstack([b,-np.inf*np.ones(2*$n), np.zeros($n)])\n")
//    }
//    else
//      {
//        wr.write(s"l = np.hstack([b,-np.inf*np.ones(2*$n),np.zeros($n), np.zeros($n)])\n")
//        wr.write(s"l = np.hstack([b,-np.inf*np.ones(2*$n),-np.inf*np.ones($n),np.zeros($n)])\n")

//      }
    wr.write(s"u = np.hstack([b,np.zeros(2*$n),np.inf*np.ones($n)])\n")
    wr.write(raw"""
prob = osqp.OSQP()
prob.setup(None, q, A, l, u, verbose=False, eps_abs=1.0e+04)
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))
""")
    (fvsmap,m,n)
  }



  def toOSQP(p: String, q: String, b: String, l: String, a: String, u: String): String = {
    raw"""
import osqp
import numpy as np
from scipy import sparse
import math
import json

P = sparse.csc_matrix($p)
q = np.array($q)
b = np.array($b)
A = sparse.csc_matrix($a)
l = np.array($l)
u = np.array($u)
prob = osqp.OSQP()
prob.setup(P, q, A, l, u, verbose=False)
res = prob.solve()
#print({"solution": res.x, "objective": res.info.obj_val})
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))
"""
  }
// added b emitter
  def emitOSQP(p: Emitter[Unit], q: Emitter[Unit], b: Emitter[Unit], l: Emitter[Unit], a: Emitter[Unit], u: Emitter[Unit]): Emitter[Unit] = {wr =>
    wr.write(raw"""
import osqp
import numpy as np
from scipy import sparse
import math
import json

""")
      wr.write("P = sparse.csc_matrix(")
      p(wr)
      wr.write(")\n")
      wr.write("q = np.array(")
      q(wr)
      wr.write(")\n")
      wr.write("b = np.array(")
      b(wr)
      wr.write(")\n")
      wr.write("A = sparse.csc_matrix(")
      a(wr)
      wr.write(")\n")
      wr.write("l = np.array(")
      l(wr)
      wr.write(")\n")
      wr.write("u = np.array(")
      u(wr)
      wr.write(")\n")
      wr.write(raw"""
prob = osqp.OSQP()
prob.setup(P, q, A, l, u,  verbose=False)
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))
""")
  }


  // specialized form where lower and upper bound matrix are equal
  // I added b emitter here which was not given earlier
  def emitOSQPEq(p: Emitter[Unit], q: Emitter[Unit],b:Emitter[Unit], a: Emitter[Unit], c: Emitter[Unit]): Emitter[Unit] = {wr =>
    wr.write(raw"""
import osqp
import numpy as np
from scipy import
import math
import json

""")
      wr.write("P = sparse.csc_matrix(")
      p(wr)
      wr.write(")\n")
      wr.write("q = np.array(")
      q(wr)
      wr.write(")\n")
      wr.write("b = np.array(")
      b(wr)
      wr.write(")\n")
      wr.write("A = sparse.csc_matrix(")
      a(wr)
      wr.write(")\n")
      wr.write("c = np.array(")
      c(wr)
      wr.write(")\n")
      wr.write(raw"""
prob = osqp.OSQP()
prob.setup(P, q, A, c, c, verbose=False)
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))
""")
  }
   
  def readProcessOutput(inputStream: InputStream): String = {
    val output = new BufferedReader(new InputStreamReader(inputStream))
    val x: java.util.List[String] = output.lines().collect(Collectors.toList())
    x.asScala.toList.mkString("\n")
  }

  def runOSQP(script: String): (List[Double],Double) = {

    val filename = "foo.py"
    val pythonname = if (new File("python.bat").exists) { "python.bat" } else { "python3" }
    val file = new File(filename)
    val writer = new PrintWriter(filename)
    writer.write(script)
    writer.close()
    
    val processBuilder = new ProcessBuilder(pythonname, file.getAbsolutePath())
    val process = processBuilder.start()
    val results = readProcessOutput(process.getInputStream())

    println(p.result)
    println(results)
    p.parseStr(p.result,results)

  }

  def parseJsonResult(json: play.api.libs.json.JsValue): (List[Double],Double) = {
    val solution = (json \ "solution").get.validate[List[Double]].getOrElse(List.empty[Double])
  val objective = (json \ "objective").get.validate[Double].getOrElse(Double.NaN)

  (solution, objective)
  }
 
  def runOSQPStreaming[A](script: Emitter[A]): (A,(List[Double],Double),Double,Double) = {

    
    val filename = "foo.py"
    val pythonname = if (new File("python.bat").exists) { "python.bat" } else { "python3" }
    val file = new File(filename)
    val writer = new PrintWriter(new BufferedWriter(new FileWriter(filename)))
    val (a,scriptTime) = Timer.timeIt(script(writer))
    //println("run OSQP Streamin a = " + a)
    writer.close()
    
    def run() = {
      val processBuilder = new ProcessBuilder(pythonname, file.getAbsolutePath())
      val process = processBuilder.start()
      Json.parse(process.getInputStream())
    }
    val (results,runTime) = Timer.timeIt(run())
    //println("Virtual Solver runOSQPStreaming results" + results)
    (a,parseJsonResult(results),scriptTime,runTime)
    //println("Virtual Solver runOSQPStreaming parseJsonResult(results)" + parseJsonResult(results))
    (a,parseJsonResult(results),scriptTime,runTime)

  }
  // TODO: Split this into a function that extends a LP/QP problem with the constraints
  // entailed by coalescing a raw table and symbolic table, and another function that finalizes the
  // system and solves it.  This will make it easier to deal with multiple tables.
  def solveList(equations: List[Database.Equation]) = {
    val (fvs,fvsTime) = Timer.timeIt(equations.foldLeft(Set[String]()){case (s,eqn) => eqn.fvsAcc(s)}.toList)
    val (lpForm,lpFormTime) = Timer.timeIt(equations.map(Database.Equation.toLPForm))
    if (lpForm.isEmpty) {
      print("No equations; skipping;")
      (null,0)
    } else if (fvs.isEmpty) {
      print("No free variables; skipping;")
      (null,0)
    } else {
      val ((a,c),solveLpFormTime) = timeIt(toLPFormSparse(lpForm))
      val (osqp,emitTime) = timeIt(emitQPSparse(fvs, a, c))
      val ((),(xs,x),_,_) = runOSQPStreaming(osqp)
      val (valuation,zipTime) = timeIt(fvs.zip(xs))
      (valuation,x/fvs.filterNot(_.startsWith("_")).length)
    }
  }

  def processList(conn: java.sql.Connection, encoder: Encoding, ctx: Database.InstanceSchema, es: Database.InstanceSchema, str: String) = {
    val q = p.parseStr(p.query,str)
    val schema = Absyn.Query.tc(ctx,q)
    val eq = encoder.queryEncoding(q)
    val equations = encoder.iterateEncodedConstraints(conn,eq,es)
    val (valuation,objective) = solveList(equations.toList)
    (valuation, objective)
  }

  def solveIter(r: Iterator[Database.Equation],flag_null:Boolean): (List[(String,Double)],Double,Int,Int,Double,Double) = {
    //println("r" + r)
    //print("c" + c)
    if (r.isEmpty) {
      (List(),0,0,0,0,0)
    } else {
      var eqn = r.next
      // print(eqn)
      //println("EmitQPSparse Iter" + emitQPSparseIter(r,c))
      var (osqp,emitTime) = timeIt(emitQPSparseIter(r,flag_null))
      //println("osqp " + osqp + "emitTime" + emitTime)
      var ((fvsmap,m,n),(xs,x),scriptTime,solveTime) = runOSQPStreaming(osqp)
      //println("Solve Iter fvsmap" + fvsmap)
      var (xs1,xs2) = xs.splitAt(n)
      //println(xs1.size)
      //println(xs2.size)
      @tailrec
      def getValuation(vs: List[Double],i: Int, acc:List[(String,Double)]): List[(String,Double)] = vs match {
        case Nil => acc
        case v::vs =>
          //print( i+1, (fvsmap._2(i),v)::acc)
          getValuation(vs,i+1,(fvsmap._2(i),v)::acc)
      }
      var (valuation,valuationTime) = timeIt(getValuation(xs2,0,List()))
      if (flag_null)
        {
          //print('n' + n)
      (valuation,x/n,m,n,emitTime+scriptTime,solveTime)

        }
      else
        {
          val (relvars,relvarTime) = timeIt(valuation.filterNot{case (v,_) => v.startsWith("_")}.length)
          //print("valuation" + valuation)
          (valuation,x/relvars,m,relvars,emitTime+scriptTime,solveTime)
        }

    }
  }

  def processIter(conn: java.sql.Connection, encoder: Encoding, ctx: Database.InstanceSchema, es: Database.InstanceSchema, str: String,flag_null:Boolean): (List[(String,Double)],Double,Int,Int,Double,Double) = {
    val q = p.parseStr(p.query,str)
    //println(q)
    val schema = Absyn.Query.tc(ctx,q)
    val eq = encoder.queryEncoding(q)
    val (enc_iter_constraints,eqCreationTime) = timeIt(encoder.iterateEncodedConstraints(conn,eq,es))
    val (valuation,objective,eqs,vars,emitTime,solveTime) = solveIter(enc_iter_constraints,flag_null)
    (valuation, objective, eqs, vars, emitTime+eqCreationTime, solveTime)
    }

  def processIter1(conn: java.sql.Connection, encoder: Encoding, ctx: Database.InstanceSchema, es: Database.InstanceSchema, str: Absyn.Query,flag_null:Boolean): (List[(String,Double)],Double,Int,Int,Double,Double) = {
    //val q = p.parseStr(p.query,str)
    val schema = Absyn.Query.tc(ctx,str)
    //println("schema" + schema)
    val eq = encoder.queryEncoding(str)
    //println(eq)
    val (enc_iter_constraints,eqCreationTime) = timeIt(encoder.iterateEncodedConstraints(conn,eq,es))
    // println("Process Iter 1 " + enc_iter_constraints)
    val (valuation,objective,eqs,vars,emitTime,solveTime) = solveIter(enc_iter_constraints,flag_null)
    //println("valuation " + valuation + "objective "  + objective)
    (valuation, objective, eqs, vars, emitTime+eqCreationTime, solveTime)
  }
 
  def solve(connector: Connector, tbl: String, encoding: Encoding,flag_null:Boolean): (List[(String,Double)],Double,Int,Int,Double,Double) = {
    val conn = connector.getConnection()
    //println("CONN" + conn)
    conn.setAutoCommit(false)
    val ctx = Database.loadSchema(conn)
    //println("CTX" + ctx)
    val es = encoding.instanceSchemaEncoding(ctx)
    //println("ES" + es)
    val result = processIter(conn, encoding, ctx, es, tbl,flag_null)
    //println("RESULT" + result)
    conn.close()
    result
 }
  def solve1(connector: Connector, tbl: Absyn.Query, encoding: Encoding, flag_null:Boolean): (List[(String,Double)],Double,Int,Int,Double,Double) = {
    val conn = connector.getConnection()
    conn.setAutoCommit(false)
    val ctx = Database.loadSchema(conn)
    val es = encoding.instanceSchemaEncoding(ctx)
    val result = processIter1(conn, encoding, ctx, es, tbl, flag_null)
    conn.close()
    result
  }

  def main(args:Array[String]) : Unit = {
    Class.forName("org.postgresql.Driver")

    val hostname = args(0)
    val dbname = args(1)
    val username = args(2)
    val password = args(3)
    val interactive = args.length < 6

    val connector = Connector(hostname,dbname,username,password)

    val encoder_to_use:Encoding = Encoding.encoder_to_use(args.applyOrElse(4,{_:Int =>"partitioning"}))

      

    if (interactive) {
      val conn = connector.getConnection()
      val ctx = Database.loadSchema(conn)
      // This just prints all the tables available in the database
      println("----->>>>> Table schemas")
      println(ctx)
      val es = encoder_to_use.instanceSchemaEncoding(ctx)
      // This prints all the eschemas of the tables distinguishing keys and values
      println("----->>>>> Encoded schemas")
      println(es)
      while (true) {
        try {
          print("solver> ")
          val (valuation,objective) = processList(conn, encoder_to_use, ctx, es, scala.io.StdIn.readLine())
          // Valuation is simply ignored in these experiments, but it is also available
          println("Objective: " + objective.toString)
        } catch {
          case Absyn.TypeError(msg) => println("Type error: " + msg)
        }
      }
    } else {
      val flag_null = true
      solve(connector, args(5), encoder_to_use, flag_null)
    }
  }
}

