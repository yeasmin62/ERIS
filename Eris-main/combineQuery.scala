import scala.util.control.Breaks._
// import EncodePartitioning._
import EncodeNF2_SparseV._
import Absyn._
import Database.Schema
import Database.Rel
import scala.collection.immutable.List
import scala.io.Source
object combineQuery {
  type query = Absyn.Query

  def combinesQuery(filename: String): Int = {
    val c = Source.fromFile(filename).getLines().size
    // println(c)
    c
  }

  def queryRet(vq: List[Absyn.Query], vt: List[String], q: Absyn.Query): query = {
    import Absyn._
    // println(spec(vt.indexOf(vtable)))

    q match {
      case Relation(r) =>
        val pos = vt.indexOf(r)
        if (pos == -1) {
          Relation(r)
        } else {
          vq(pos)
        }
//        case
//        case (v, c) if r == v vq(vt.indexOf(r))
//        case (v, c) if r != v && vt.contains(r) => vq(vt.indexOf(r))
//        case (v, c) if r != v && !vt.contains(r) => Relation(r)
//        case _ => throw NYI
//      }
      case Select(q, p) =>
        val q0 = queryRet(vq, vt, q)
        Select(q0, p)

      case ProjectAway(q, fs) =>
        ProjectAway(queryRet(vq, vt, q), fs)
      case Aggregation(q, grouped, aggregated) =>
        Aggregation(queryRet(vq, vt, q), grouped, aggregated)
      case NaturalJoin(q1, q2) =>
        // println(q10)
        // println(q11)
        NaturalJoin(queryRet(vq, vt, q1), queryRet(vq, vt, q2))
      case Project(q, fs) =>
        Project(queryRet(vq, vt, q), fs)

      case Rename(q, renaming) =>
        Rename(queryRet(vq, vt, q), renaming)
      case UnionAll(q1, q2) =>
        UnionAll(queryRet(vq, vt, q1), queryRet(vq, vt, q2))

      case DUnion(q1, q2, attr) =>
        DUnion(queryRet(vq, vt, q1), queryRet(vq, vt, q2), attr)

      case Derivation(q, dvar) =>
        Derivation(queryRet(vq, vt, q), dvar)

      case Coalesce(q, collapse) =>
        Coalesce(queryRet(vq, vt, q), collapse)

      case Derivation(q, List((f, Times(Var(a), Num(c))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        (if (schema.varfreeFields.contains(a)) {
           Derivation(q0, List((f, Times(Var(a), Num(c)))))
         } else {
           Derivation(q0, List((f, ArrayScalar(Var(a), Num(c)))))
         })
      case Derivation(q, List((f, Num(c)))) =>
        val q0 = queryRet(vq, vt, q)
        (Derivation(q0, List((f, Num(c)))))

      case Derivation(q, List((f, Var(a)))) =>
        val q0 = queryRet(vq, vt, q)
        (Derivation(q0, List((f, Var(a)))))

      case Derivation(q, List((f, Times(Var(a), Num(c))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        (if (schema.varfreeFields.contains(a)) {
           Derivation(q0, List((f, Times(Var(a), Num(c)))))
         } else {
           Derivation(q0, List((f, ArrayScalar(Var(a), Num(c)))))
         })

      case Derivation(q, List((f, Times(Num(c), Var(a))))) =>
        queryRet(vq, vt, Derivation(q, List((f, Times(Var(a), Num(c))))))

      case Derivation(q, List((b, Times(UMinus(Num(c)), Var(a))))) =>
        queryRet(vq, vt, Derivation(q, List((b, Times(Num(-c), Var(a))))))

      case Derivation(q, List((f, Times(Var(a), Var(b))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        // The fourth case should not happen, because it is not a linear expression
        if (
          schema.varfreeFields.contains(b) && !schema.varfreeFields.contains(a)
        ) {
          (Derivation(q0, List((f, ArrayScalar(Var(a), Var(b))))))
        } else if (
          schema.varfreeFields.contains(a) && !schema.varfreeFields.contains(b)
        ) {
          (Derivation(q0, List((f, ArrayScalar(Var(b), Var(a))))))
        } else { // (schema.varfreeFields.contains(a) && schema.varfreeFields.contains(b))
          (Derivation(q0, List((f, Times(Var(b), Var(a))))))
        }

      case Derivation(q, List((f, Div(Var(a), Num(c))))) =>
        queryRet(vq, vt, Derivation(q, List((f, Times(Var(a), Num(1 / c))))))

      // This case assumes the second variable is actually constant
      case Derivation(q, List((f, Div(Num(c), Var(a))))) =>
        val q0 = queryRet(vq, vt, q)
        (Derivation(q0, List((f, Times(Num(c), Inv(Var(a)))))))

      // This case assumes the second variable is actually constant
      case Derivation(q, List((f, Div(Var(a), Var(b))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        if (schema.varfreeFields.contains(a)) {
          (Derivation(q0, List((f, Times(Var(a), Inv(Var(b)))))))
        } else {
          (Derivation(q0, List((f, ArrayScalar(Var(a), Inv(Var(b)))))))
        }

      case Derivation(q, List((f, Plus(Var(a), Num(c))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        if (schema.varfreeFields.contains(a)) {
          (Derivation(q0, List((f, Plus(Var(a), Num(c))))))
        } else {
          (Derivation(
            q0,
            List((f, ArrayPlus(Var(a), CreateSparseVector("", c.toString))))
          ))
        }

      case Derivation(q, List((f, Plus(Num(c), Var(a))))) =>
        queryRet(vq, vt, Derivation(q, List((f, Plus(Var(a), Num(c))))))

      case Derivation(q, List((f, Plus(Var(a), Var(b))))) =>
        val q0 = queryRet(vq, vt, q)
        val schema = baseSchema(q.schema)
        if (
          schema.varfreeFields.contains(b) && schema.varfreeFields.contains(a)
        ) {
          (Derivation(q0, List((f, Plus(Var(a), Var(b))))))
        } else if (
          !schema.varfreeFields.contains(a) && !schema.varfreeFields.contains(b)
        ) {
          (Derivation(q0, List((f, ArrayPlus(Var(b), Var(a))))))
        } else if (
          !schema.varfreeFields.contains(a) && schema.varfreeFields.contains(b)
        ) {
          (Derivation(
            q0,
            List((f, ArrayPlus(Var(a), CreateSparseVector("", b))))
          ))
        } else {
          queryRet(vq, vt, Derivation(q, List((f, Plus(Var(b), Var(a))))))
        }

    }

  }
}
