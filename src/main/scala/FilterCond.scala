import scala.language.implicitConversions

// Row = Map[String, String]
trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // gets the element from the map where key == colName
    val miniMap: Row = r.filter(elem => elem._1 == colName)

    // verifies predicate for the corresponding element
    if (predicate(miniMap.values.head)) Some(true)
    else Some(false)
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // evaluate all the expressions
    val result = conditions.map(cond => cond.eval(r))

    // use foldLeft to combine all results
    val finalResult: Option[Boolean] = result.foldLeft(Option(true))((acc, elem) => {
      (acc, elem) match {
        case (Some(a), Some(b)) => Some(op(a, b))
        case _ => None
      }
    })
    finalResult
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // evaluate the expression
    val result: Option[Boolean] = f.eval(r)

    // return the opposite
    result match {
      case Some(true) => Some(false)
      case Some(false) => Some(true)
      case None => None
    }
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = {
  case class AndHelper() extends FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      val e1 = f1.eval(r)
      val e2 = f2.eval(r)
      (e1, e2) match {
        case (Some(a), Some(b)) => Some(a && b)
        case _ => None
      }
    }
  }
  AndHelper()
}

def Or(f1: FilterCond, f2: FilterCond): FilterCond = {
  case class OrHelper() extends FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      val e1 = f1.eval(r)
      val e2 = f2.eval(r)
      (e1, e2) match {
        case (Some(a), Some(b)) => Some(a || b)
        case _ => None
      }
    }
  }
  OrHelper()
}

def Equal(f1: FilterCond, f2: FilterCond): FilterCond = {
  case class EqualHelper() extends FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      val e1 = f1.eval(r)
      val e2 = f2.eval(r)
      (e1, e2) match {
        case (Some(a), Some(b)) => Some(a == b)
        case _ => None
      }
    }
  }
  EqualHelper()
}

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // evaluate conditions and filter them based on true values
    val evaluatedCond = fs.filter(cond => cond.eval(r).contains(true))

    // if the list becomes empty, then no values are true => false, else => true
    if (evaluatedCond.isEmpty) Some(false)
    else Some(true)
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    // evaluate conditions and filter them based on false values
    val evaluatedCond = fs.filter(cond => cond.eval(r).contains(false))

    // if the list becomes empty, then all values are true => true, else => false
    if (evaluatedCond.isEmpty) Some(true)
    else Some(false)
  }
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}