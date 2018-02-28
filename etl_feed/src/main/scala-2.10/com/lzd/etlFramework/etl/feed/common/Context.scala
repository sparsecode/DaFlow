package com.lzd.etlFramework.etl.feed.common

object ContextConstantEnum extends Enumeration{
  type constant = Value
  val VENTURE, FIRST_DATE, SECOND_DATE,
      HADOOP_CONF, SPARK_CONTEXT, SQL_CONTEXT, HIVE_CONTEXT,
      JOB_STATIC_PARAM, EXTRACT, TRANSFORM, LOAD, SCHEMA = Value
}

object Context {
  private var contextualObjects = Map[ContextConstantEnum.constant, Any]()

  def addContextualObject[T](key: ContextConstantEnum.constant, obj: T): Unit = {
    contextualObjects +=(key -> obj)
  }

  def getContextualObject[T](key: ContextConstantEnum.constant): T = {
    val output = contextualObjects.getOrElse(key,None)
    output.asInstanceOf[T]
  }
}
