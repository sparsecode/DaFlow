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

import scala.util.parsing.combinator._

class SQLParser extends JavaTokenParsers {
  def sqlIdent: Parser[String] =
    """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*([.-]\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)?""".r
  def sqlDecimalNumber: Parser[String] = """-?(\d+(\.\d*)?|\d*\.\d+)""".r

  def stringLiteral1: Parser[String] =
    ("'" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r
  def sqlLiterals: SQLParser.this.Parser[String] =
    stringLiteral | stringLiteral1

  def cmpExpr: Parser[String] = "=" | ">=" | "<=" | "<>" | "!=" | ">" | "<"
  def boolean: SQLParser.this.Parser[Boolean] =
    "true" ^^^ "true".toBoolean | "false" ^^^ "false".toBoolean

  def query: Parser[Query] = operation ~ from ~ opt(where) ~ opt(order) ^^ {
    case operation ~ from ~ where ~ order =>
      Query(from, operation, where, order)
  }

  def operation: Parser[Select] = {
    ("select" | "update" | "delete") ~ repsep(sqlIdent, ",") ^^ {
      case "select" ~ f => Select(f: _*)
      case _ => throw new IllegalArgumentException("Operation not implemented")
    }
  }

  def from: Parser[From] = "from" ~> sqlIdent ^^ (From(_))

  def where: Parser[Where] = "where" ~> rep(clause) ^^ (Where(_: _*)) ^^ {
    where =>
      where
  }

  def clause: Parser[Clause] = (predicate | parens) * (
    ("and" | "AND") ^^^ { (a: Clause, b: Clause) =>
      And(a, b)
    }
      | ("or" | "OR") ^^^ { (a: Clause, b: Clause) =>
        Or(a, b)
      }
  )

  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def predicate: SQLParser.this.Parser[ValueClause] = (
    sqlIdent ~ cmpExpr ~ boolean ^^ {
      case f ~ cmp_expr ~ b => BooleanExpressions(f, cmp_expr, b)
    }
      | sqlIdent ~ cmpExpr ~ sqlLiterals ^^ {
        case f ~ cmp_expr ~ v => StringExpressions(f, cmp_expr, stripQuotes(v))
      }
      | sqlIdent ~ cmpExpr ~ sqlDecimalNumber ^^ {
        case f ~ cmp_expr ~ i => NumberExpressions(f, cmp_expr, i.toDouble)
      }
      | sqlIdent ~ ("between" | "BETWEEN") ~ sqlLiterals ~ ("and" | "AND") ~ sqlLiterals ^^ {
        case f ~ ("between" | "BETWEEN") ~ i ~ ("and" | "AND") ~ j =>
          Between(f, stripQuotes(i), stripQuotes(j))
      }
      | sqlIdent ~ ("between" | "BETWEEN") ~ sqlDecimalNumber ~ ("and" | "AND") ~ sqlDecimalNumber ^^ {
        case f ~ ("between" | "BETWEEN") ~ i ~ ("and" | "AND") ~ j =>
          Between(f, i.toDouble, j.toDouble)
      }
      | sqlIdent ~ ("like" | "LIKE") ~ sqlLiterals ^^ {
        case f ~ ("like" | "LIKE") ~ v => Like(f, stripQuotes(v))
      }
      | sqlIdent ~ ("IS" | "is") ~ ("not" | "NOT") ~ ("null" | "NULL") ^^ {
        case f ~ ("IS" | "is") ~ ("not" | "NOT") ~ ("null" | "NULL") =>
          NotNull(f)
      }
      | sqlIdent ~ ("IS" | "is") ~ ("null" | "NULL") ^^ {
        case f ~ ("IS" | "is") ~ ("null" | "NULL") => Null(f)
      }
  )

  def order: Parser[Direction] = {
    "order" ~> "by" ~> sqlIdent ~ ("asc" | "desc") ^^ {
      case f ~ "asc" => Asc(f)
      case f ~ "desc" => Desc(f)
    }
  }

  def stripQuotes(s: String): String = s.substring(1, s.length - 1)

  def parseWhere(sql: String): Option[Where] = {
    val output = parseAll(where, sql)
    if(output.successful) { Option(output.get) }
    else { None }
  }

  def parseSelect(sql: String): Option[Select] = {
    val output = parseAll(operation, sql)
    if(output.successful) { Option(output.get) }
    else { None }
  }

  def getSelectClause(clauseString: String): Option[Seq[String]] = {
    val select: Option[Select] = parseSelect("select " + clauseString)
    if (select.isDefined) { Some(select.get.fields) }
    else { None }
  }

  def getWhereClause(clauseString: String): Option[Seq[Clause]] = {
    val where: Option[Where] = parseWhere(sql = "where " + clauseString)
    if (where.isDefined) {
      val out = scala.collection.mutable.ListBuffer[Clause]()
      def recurClause(clause: Clause): Clause = {
        if (clause.getFields.contains("rClause")) {
          recurClause(clause.getFields("rClause").asInstanceOf[Clause])
        }
        if (clause.getFields.contains("lClause")) {
          recurClause(clause.getFields("lClause").asInstanceOf[Clause])
        }
        out += clause
        clause
      }
      recurClause(where.get.clauses.head)
      Some(out.filter(_.hasField))
    } else { None }
  }
}
