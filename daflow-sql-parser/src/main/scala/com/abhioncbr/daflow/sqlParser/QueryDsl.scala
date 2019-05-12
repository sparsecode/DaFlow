/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.abhioncbr.daflow.sqlParser

abstract class Operation { def from(table: String): From = From(table, Option(this)) }
case class Select(fields: String*) extends Operation
case class From(table: String, operation: Option[Operation] = None) {
  def where(clauses: Clause*): Query = Query(from = this, operation = operation.get, Option(Where(clauses: _*)))
}

case class Query(from: From, operation: Operation, where: Option[Where], order: Option[Direction] = None) {
  def order(dir: Direction): Query = this.copy(order = Option(dir))
}

case class Where(clauses: Clause*)

abstract class Clause {
  def hasField: Boolean
  def getFields: Map[String, Any]
  def and(otherField: Clause): Clause = And(this, otherField)
  def or(otherField: Clause): Clause = Or(this, otherField)
}
abstract class ValueClause(t: String, f: String, values: Any*) extends Clause {
  override def hasField: Boolean = true
  override def getFields: Map[String, Any] = Map("type" -> t, "field" -> f, "value" -> values)
}
abstract class ExpressionValueClause(t: String, f: String, expr: String, values: Any*) extends ValueClause(t, f,
  values) {
  override def hasField: Boolean = true
  override def getFields: Map[String, Any] = Map("type" -> t, "field" -> f, "value" -> values, "expr" -> expr)
}
abstract class ReferenceClause(t: String, lClause: Clause, rClause: Clause) extends Clause {
  override def hasField: Boolean = false
  override def getFields: Map[String, Any] = Map("type" -> t, "lClause" -> lClause, "rClause" -> rClause)
}

case class Null(f: String) extends ValueClause("Null", f)
case class NotNull(f: String) extends ValueClause("NotNull", f)
case class Like(f: String, value: Any) extends ValueClause("Like", f, value)
case class In(f: String, values: String*) extends ValueClause("in", f, values)
case class Between(f: String, values: Any*) extends ValueClause("Between", f, values)
case class StringExpressions(f: String, expr: String, value: String)
  extends ExpressionValueClause("stringEquals", f, expr: String, value)
case class NumberExpressions(f: String, expr: String, value: Number)
  extends ExpressionValueClause("numberEquals", f, expr: String, value)
case class BooleanExpressions(f: String, expr: String, value: Boolean)
  extends ExpressionValueClause("booleanEquals", f, expr, value)

case class And(lClause: Clause, rClause: Clause) extends ReferenceClause("and", lClause, rClause)
case class Or(lClause: Clause, rClause: Clause) extends ReferenceClause("or", lClause, rClause)

abstract class Direction
case class Asc(field: String) extends Direction
case class Desc(field: String) extends Direction
