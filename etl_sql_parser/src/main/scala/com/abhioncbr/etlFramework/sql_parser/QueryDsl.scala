package com.abhioncbr.etlFramework.sql_parser

case class Query(operation:Operation,  from: From,  where: Option[Where],  order: Option[Direction] = None) {
  def order(dir: Direction): Query = this.copy(order = Option(dir))
}

abstract class Operation {
  def from(table: String) = From(table, Option(this))
}
case class Select( fields:String*) extends Operation
case class From( table: String,  operation:Option[Operation] = None) {
  def where(clauses: Clause*): Query = Query(operation.get, this, Option(Where(clauses: _*)))
}

case class Where( clauses: Clause*)

abstract class Clause {
  def hasField:Boolean
  def getFields:Map[String,Any]
  def and(otherField: Clause): Clause = And(this, otherField)
  def or(otherField: Clause): Clause = Or(this, otherField)
}
abstract class ValueClause(t: String, f: String, values: Any*) extends Clause {
  override def hasField: Boolean = true
  override def getFields: Map[String,Any] = Map("type" -> t, "field" -> f, "value" -> values)
}
abstract class ReferenceClause(t: String, lClause:Clause, rClause:Clause) extends Clause {
  override def hasField: Boolean = false
  override def getFields: Map[String,Any] = Map("type" -> t, "lClause" -> lClause, "rClause" -> rClause)
}

case class Null(f: String) extends ValueClause("Null", f)
case class NotNull(f: String) extends ValueClause("NotNull", f)
case class Like(f: String, value: Any) extends ValueClause("Like", f, value)
case class Between(f: String, values: Any*) extends ValueClause("Between", f, values)
case class StringEquals( f: String,  value: String) extends ValueClause("stringEquals", f, value)
case class NumberEquals( f: String,  value: Number) extends ValueClause("numberEquals", f, value)
case class BooleanEquals( f: String,  value: Boolean) extends ValueClause("booleanEquals", f, value)
case class In( f: String,  values: String*) extends ValueClause("in", f, values)
case class And(lClause:Clause,  rClause:Clause) extends ReferenceClause("and", lClause, rClause )
case class Or(lClause:Clause,  rClause:Clause) extends ReferenceClause("or", lClause, rClause )

abstract class Direction
case class Asc(field: String) extends Direction
case class Desc(field: String) extends Direction