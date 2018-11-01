package com.abhioncbr.etlFramework.commons

object ContextConstantEnum extends Enumeration{
  type constant = Value
  val FIRST_DATE, SECOND_DATE,
  HADOOP_CONF, SPARK_CONTEXT, SQL_CONTEXT,
  JOB_STATIC_PARAM_CONF, EXTRACT_CONF, TRANSFORM_CONF, LOAD_CONF,
  SCHEMA, OTHER_PARAM = Value
}
