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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SqlParserSpec extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val fieldLit: String = "field"
  private val valueLit: String = "value"
  private val typeLit: String = "type"

  "getSelectClause1" should "return list with one column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] =
      parser.getSelectClause(clauseString = "data_data ")
    output should not be None
    output should be(Some(List("data_data")))
  }

  "getSelectClause2" should "return list with nine column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString =
      "event, facebook_id, google_id, http-agent, ip_address, user_email, date_time, user_id, version")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(9)
  }

  "getSelectClause3" should "return list with zero column " in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString = "")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(0)
  }

  "getSelectClause4" should "return list with one column having name of one character" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString = "z")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(1)
    output.get.head should be("z")
  }

  "getSelectClause5" should "return list with one column having name of one character with table name" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString = "x.y")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(1)
    output.get.head should be("x.y")
  }

  "getSelectClause6" should "return list with three column having name with table name" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(
      clauseString = "data_data, data.products, products ")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(3)
    output.get.head should be("data_data")
    output.get(1) should be("data.products")
    output.get.last should be("products")
  }

  "getSelectClause7" should "return list with three column having name with table name and no space between them" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] =
      parser.getSelectClause(clauseString = "data_data,data.products,products")
    output should not be None
    output.isDefined should be(true)
    output.get.length should be(3)
    output.get.head should be("data_data")
    output.get(1) should be("data.products")
    output.get.last should be("products")
  }

  "getSelectClause8" should "return list with multiple columns" in {
    val parser = new SQLParser
    val output: Option[Seq[String]] = parser.getSelectClause(clauseString =
      "data.order_id, data.order_nr, data.anonymous_id, data.user_id, data.session_id, " +
        "data.source, data.products, data.payment_type, data.fingerprint, data.device_id")
    output should not be None
    output.isDefined should be(true)
    output.get.head should be("data.order_id")
  }

  "getWhereClause1" should "return list with clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "m= 'n'")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("m")
    output.get.head.getFields(valueLit) should be(Array("n"))
    output.get.head.getFields(typeLit) should be("stringEquals")
  }

  "getWhereClause2" should "return list with clause and wothout space" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "p='q'")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("p")
    output.get.head.getFields(valueLit) should be(Array("q"))
    output.get.head.getFields(typeLit) should be("stringEquals")
  }

  "getWhereClause3" should "return list with clause and have value 1.1" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "z= 1.1")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("z")
    output.get.head.getFields(valueLit) should be(Array(1.1))
    output.get.head.getFields(typeLit) should be("numberEquals")
  }

  "getWhereClause4" should "return list with clause and have value 1.1, without space" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "c=1.1")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("c")
    output.get.head.getFields(valueLit) should be(Array(1.1))
    output.get.head.getFields(typeLit) should be("numberEquals")
  }

  "getWhereClause5" should "return list with clause and have field name with dot" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a.b=1.1")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a.b")
    output.get.head.getFields(valueLit) should be(Array(1.1))
    output.get.head.getFields(typeLit) should be("numberEquals")
  }

  "getWhereClause6" should "return list with numberEquals clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "records.like = 1")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("records.like")
    output.get.head.getFields(valueLit) should be(Array(1))
    output.get.head.getFields(typeLit) should be("numberEquals")
  }

  "getWhereClause7" should "return list with LIKE clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "hostname like 'my%'")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("hostname")
    output.get.head.getFields(valueLit) should be(Array("my%"))
    output.get.head.getFields(typeLit) should be("Like")
  }

  "getWhereClause8" should "return list with LIKE clause with databaseName & tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a.b like 'my%'")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a.b")
    output.get.head.getFields(valueLit) should be(Array("my%"))
    output.get.head.getFields(typeLit) should be("Like")
  }

  "getWhereClause9" should "return list with LIKE clause with one character tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a like 'my%'")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a")
    output.get.head.getFields(valueLit) should be(Array("my%"))
    output.get.head.getFields(typeLit) should be("Like")
  }

  "getWhereClause10" should "return list with NULL clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "records.like IS NULL")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("records.like")
    output.get.head.getFields(typeLit) should be("Null")
  }

  "getWhereClause11" should "return list with NULL clause and name have databaseName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a.b IS NULL")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a.b")
    output.get.head.getFields(typeLit) should be("Null")
  }

  "getWhereClause12" should "return list with NotNull clause and name have databaseName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a.b IS NOT NULL")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a.b")
    output.get.head.getFields(typeLit) should be("NotNull")
  }

  "getWhereClause13" should "return list with Null clause and one character tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a IS NULL")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a")
    output.get.head.getFields(typeLit) should be("Null")
  }

  "getWhereClause14" should "return list with Null clause and underscore in tableName" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a_b IS NULL")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a_b")
    output.get.head.getFields(typeLit) should be("Null")
  }

  "getWhereClause15" should "return list with Between clause" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "records_red between 10 and 20")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("records_red")
    output.get.head.getFields(typeLit) should be("Between")
    output.get.head.getFields(valueLit) should be(Array(Array(10.0, 20.0)))
  }

  "getWhereClause16" should "return list with Between clause with decimal numbers" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(
      clauseString = "records_red between 10.11 and 20.11")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("records_red")
    output.get.head.getFields(typeLit) should be("Between")
    output.get.head.getFields(valueLit) should be(Array(Array(10.11, 20.11)))
  }

  "getWhereClause17" should "return list with Between clause with decimal numbers" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] =
      parser.getWhereClause(clauseString = "a between 0.11 and -1.32")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a")
    output.get.head.getFields(typeLit) should be("Between")
    output.get.head.getFields(valueLit) should be(Array(Array(0.11, -1.32)))
  }

  "getWhereClause18" should "return list with Between clause with strings" in {
    val parser = new SQLParser
    val output: Option[Seq[Clause]] = parser.getWhereClause(clauseString = "a between \"a\" AND \"b\"")
    output should not be None
    output.isDefined should be(true)
    output.get.head.hasField should be(true)
    output.get.head.getFields(fieldLit) should be("a")
    output.get.head.getFields(typeLit) should be("Between")
    output.get.head.getFields(valueLit) should be(Array(Array("a", "b")))
  }
}
