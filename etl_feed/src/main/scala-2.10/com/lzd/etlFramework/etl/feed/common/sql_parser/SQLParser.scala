package com.lzd.etlFramework.etl.feed.common.sql_parser

import com.lzd.etlFramework.etl.feed.Logger
import org.joda.time.format.DateTimeFormat

import scala.util.parsing.combinator._

class SQLParser extends JavaTokenParsers {
  def sqlIdent: Parser[String] = """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*([.-]\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)?""".r
  def sqlDecimalNumber: Parser[String] = """-?(\d+(\.\d*)?|\d*\.\d+)""".r

  def stringLiteral1: Parser[String] = ("'"+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*"""+"'").r
  def sqlLiterals = stringLiteral | stringLiteral1

  def cmp_expr:Parser[String] = "=" | ">=" | "<=" | "<>" | "!=" | ">" | "<"
  def boolean = "true" ^^^ "true".toBoolean | "false" ^^^ "false".toBoolean

  def query:Parser[Query] = operation ~ from ~ opt(where) ~ opt(order) ^^ {
    case operation ~ from ~ where ~ order => Query(operation, from, where, order)
  }

  def operation:Parser[Select] = {
    ("select" | "update" | "delete") ~ repsep(sqlIdent, ",") ^^ {
      case "select" ~ f => Select(f:_*)
      case _ => throw new IllegalArgumentException("Operation not implemented")
    }
  }

  def from:Parser[From] = "from" ~> sqlIdent ^^ (From(_))

  def where:Parser[Where] = "where" ~> rep(clause) ^^ (Where(_:_*)) ^^ { where => where }

  def clause:Parser[Clause] = (predicate|parens) * (
    ("and" | "AND") ^^^ { (a:Clause, b:Clause) => And(a,b) }
    | ("or" | "OR") ^^^ { (a:Clause, b:Clause) => Or(a,b) }
    )

  def parens:Parser[Clause] = "(" ~> clause  <~ ")"

  def predicate = (
    sqlIdent ~ cmp_expr ~ boolean ^^ { case f ~ cmp_expr ~ b => BooleanEquals(f,b)}
    | sqlIdent ~ cmp_expr ~ sqlLiterals ^^ { case f ~ cmp_expr ~ v => StringEquals(f,stripQuotes(v))}
    | sqlIdent ~ cmp_expr ~ sqlDecimalNumber ^^ { case f ~ cmp_expr ~ i => NumberEquals(f,i.toDouble)}
    | sqlIdent ~ ("between" | "BETWEEN") ~ sqlLiterals ~ ("and" | "AND") ~ sqlLiterals ^^ { case f ~ ("between" | "BETWEEN") ~ i ~ ("and" | "AND") ~ j => Between(f, stripQuotes(i) , stripQuotes(j))}
    | sqlIdent ~ ("between" | "BETWEEN") ~ sqlDecimalNumber ~ ("and" | "AND") ~ sqlDecimalNumber ^^ { case f ~ ("between" | "BETWEEN") ~ i ~ ("and" | "AND") ~ j => Between(f,i.toDouble,j.toDouble)}
    | sqlIdent ~ ("like" | "LIKE") ~ sqlLiterals ^^ {case f ~ ("like" | "LIKE") ~ v => Like(f,stripQuotes(v))}
    | sqlIdent ~ ("IS" | "is") ~ ("not" | "NOT")  ~ ("null" | "NULL") ^^ {case f ~ ("IS" | "is") ~ ("not" | "NOT") ~ ("null" | "NULL") => NotNull(f)}
    | sqlIdent ~ ("IS" | "is") ~ ("null" | "NULL") ^^ {case f ~ ("IS" | "is") ~ ("null" | "NULL") => Null(f)}
    )

  def order:Parser[Direction] = {
    "order" ~> "by" ~> sqlIdent  ~ ("asc" | "desc") ^^ {
      case f ~ "asc" => Asc(f)
      case f ~ "desc" => Desc(f)
    }
  }

  def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parseWhere(sql:String):Option[Where] = {
    parseAll(where, sql) match {
      case Success(r, q) => Option(r)
      case x => Logger.log.error(x); None
    }
  }

  def parseSelect(sql:String): Option[Select] = {
    parseAll(operation, sql) match {
      case Success(r, q) => Option(r)
      case x => Logger.log.error(x); None
    }
  }

  def getSelectClause(clauseString: String): Option[Seq[String]] = {
    val select: Option[Select] = parseSelect("select " + clauseString)
    if(select.isDefined) return Some(select.get.fields)
    None
  }

  def getWhereClause(clauseString: String): Option[Seq[Clause]] = {
    val where: Option[Where] = parseWhere("where " + clauseString)
    if(where.isDefined) {
      val out = scala.collection.mutable.ListBuffer[Clause]()

      def recurClause(clause: Clause): Clause = {
        if(clause.getFields.contains("rClause")) { recurClause(clause.getFields("rClause").asInstanceOf[Clause] )}
        if(clause.getFields.contains("lClause")) { recurClause(clause.getFields("lClause").asInstanceOf[Clause] )}
        out += clause
        clause
      }
      recurClause(where.get.clauses.head)
      return Some(out.filter(_.hasField))
    }
    None
  }
}

object SQLParser extends App{
  val parser = new SQLParser
  println(parser.getSelectClause("data_data ").get)
  println(parser.getSelectClause("event, facebook_id, google_id, http-agent, ip_address, user_email, date_time, user_id, version, lazada_rest_proxy_event_timestamp").get)
  println(parser.getSelectClause("a").get)
  println(parser.getSelectClause("a.b").get)
  println(parser.getSelectClause("data_data, data.products, products ").get)
  println(parser.getSelectClause("data_data,data.products,products ").get)
  println(parser.getSelectClause("data.order_id, data.order_nr, data.anonymous_id, data.user_id, data.session_id, data.source, data.products, data.payment_type, data.fingerprint, data.device_id").get)
  println(parser.getWhereClause("a= 'b'").get)
  println(parser.getWhereClause("a = 1.1").get)
  println(parser.getWhereClause("a.b = 1").get)
  println(parser.getWhereClause("records.like = 1").get)
  println(parser.getWhereClause("hostname like 'my%'").get)
  println(parser.getWhereClause("records.like IS NULL").get)
  println(parser.getWhereClause("a.b IS NULL").get)
  println(parser.getWhereClause("a.b IS NOT NULL").get)
  println(parser.getWhereClause("a.b LIKE '%aa'").get)
  println(parser.getWhereClause("a LIKE '%aa'").get)
  println(parser.getWhereClause("a IS NULL").get)
  println(parser.getWhereClause("records_red  IS NULL").get)
  println(parser.getWhereClause("records_red between 10 and 20").get)
  println(parser.getWhereClause("records_red between 10.11 and 20.12").get)
  println(parser.getWhereClause("a between 0.11 and -1.32").get)
  println(parser.getWhereClause(s"""a between "a" AND "b"""").get)

  val DatePattern = "yyyy-MM-dd HH:mm:ss"
  val dateParser  = DateTimeFormat.forPattern(DatePattern)
  val first_date = dateParser.parseDateTime("2017-02-26 23:00:00")
  println(first_date.year.get)
  println(first_date.monthOfYear.get)
  println(first_date.dayOfMonth.get)
  println(first_date.hourOfDay.get())


  //known issue ~ not supporting
  //println(parser.getWhereClause(s"""a between 'a' AND 'b'""").get)
}