package com.abhioncbr.etlFramework.sqlParser

import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class SqlParserSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  "getSelectClause" should "return list with one column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString = "data_data ")
    output should not be null
    output should be (Some(List("data_data")))
  }

  "getSelectClause" should "return list with nine column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "event, facebook_id, google_id, http-agent, ip_address, user_email, date_time, user_id, version")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (9)
  }

  "getSelectClause" should "return list with zero column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (0)
  }

  "getSelectClause" should "return list with one column having name of one character" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "a")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (1)
    output.get.head should be ("a")
  }

  "getSelectClause" should "return list with one column having name of one character with table name" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "a.b")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (1)
    output.get.head should be ("a.b")
  }

  "getSelectClause" should "return list with three column having name with table name" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "data_data, data.products, products ")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (3)
    output.get.head should be ("data_data")
    output.get(1) should be ("data.products")
    output.get.last should be ("products")
  }

  "getSelectClause" should "return list with three column having name with table name and no space between them" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "data_data,data.products,products")
    output should not be null
    output.isDefined should be (true)
    output.get.length should be (3)
    output.get.head should be ("data_data")
    output.get(1) should be ("data.products")
    output.get.last should be ("products")
  }

  "getSelectClause" should "return list with multiple columns" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause( clauseString = "data.order_id, data.order_nr, data.anonymous_id, data.user_id, data.session_id, data.source, data.products, data.payment_type, data.fingerprint, data.device_id")
    output should not be null
    output.isDefined should be (true)
    output.get.head should be ("data.order_id")
  }

  "getWhereClause" should "return list with clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a= 'b'")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("value") should be (Array("b"))
    output.get.head.getFields("type") should be ("stringEquals")
  }

  "getWhereClause" should "return list with clause and wothout space" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a='b'")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("value") should be (Array("b"))
    output.get.head.getFields("type") should be ("stringEquals")
  }

  "getWhereClause" should "return list with clause and have value 1.1" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a= 1.1")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("value") should be (Array(1.1))
    output.get.head.getFields("type") should be ("numberEquals")
  }

  "getWhereClause" should "return list with clause and have value 1.1, without space" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a=1.1")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("value") should be (Array(1.1))
    output.get.head.getFields("type") should be ("numberEquals")
  }

  "getWhereClause" should "return list with clause and have field name with dot" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a.b=1.1")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a.b")
    output.get.head.getFields("value") should be (Array(1.1))
    output.get.head.getFields("type") should be ("numberEquals")
  }

  "getWhereClause" should "return list with numberEquals clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "records.like = 1")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("records.like")
    output.get.head.getFields("value") should be (Array(1))
    output.get.head.getFields("type") should be ("numberEquals")
  }

  "getWhereClause" should "return list with LIKE clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "hostname like 'my%'")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("hostname")
    output.get.head.getFields("value") should be (Array("my%"))
    output.get.head.getFields("type") should be ("Like")
  }

  "getWhereClause" should "return list with LIKE clause with databaseName & tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a.b like 'my%'")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a.b")
    output.get.head.getFields("value") should be (Array("my%"))
    output.get.head.getFields("type") should be ("Like")
  }

  "getWhereClause" should "return list with LIKE clause with one character tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a like 'my%'")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("value") should be (Array("my%"))
    output.get.head.getFields("type") should be ("Like")
  }

  "getWhereClause" should "return list with NULL clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "records.like IS NULL")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("records.like")
    output.get.head.getFields("type") should be ("Null")
  }

  "getWhereClause" should "return list with NULL clause and name have databaseName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a.b IS NULL")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a.b")
    output.get.head.getFields("type") should be ("Null")
  }

  "getWhereClause" should "return list with NotNull clause and name have databaseName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a.b IS NOT NULL")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a.b")
    output.get.head.getFields("type") should be ("NotNull")
  }

  "getWhereClause" should "return list with Null clause and one character tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a IS NULL")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("type") should be ("Null")
  }

  "getWhereClause" should "return list with Null clause and underscore in tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a_b IS NULL")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a_b")
    output.get.head.getFields("type") should be ("Null")
  }

  "getWhereClause" should "return list with Between clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "records_red between 10 and 20")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("records_red")
    output.get.head.getFields("type") should be ("Between")
    output.get.head.getFields("value") should be (Array(Array(10.0,20.0)))
  }

  "getWhereClause" should "return list with Between clause with decimal numbers" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "records_red between 10.11 and 20.11")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("records_red")
    output.get.head.getFields("type") should be ("Between")
    output.get.head.getFields("value") should be (Array(Array(10.11,20.11)))
  }

  "getWhereClause1" should "return list with Between clause with decimal numbers" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a between 0.11 and -1.32")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("type") should be ("Between")
    output.get.head.getFields("value") should be (Array(Array(0.11,-1.32)))
  }

  "getWhereClause" should "return list with Between clause with strings" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = s"""a between "a" AND "b"""")
    output should not be null
    output.isDefined should be (true)
    output.get.head.hasField should be (true)
    output.get.head.getFields("field") should be ("a")
    output.get.head.getFields("type") should be ("Between")
    output.get.head.getFields("value") should be (Array(Array("a","b")))
  }
}
