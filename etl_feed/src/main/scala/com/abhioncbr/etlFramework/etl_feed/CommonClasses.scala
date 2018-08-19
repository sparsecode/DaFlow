package com.abhioncbr.etlFramework.etl_feed

import java.io.OutputStreamWriter

import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

case class JobResult(success: Boolean, subtask: String,
                     transformationPassedCount: Long,
                     transformationFailedCount: Long,
                     validateCount: Long, nonValidatedCount: Long, failureReason: String)

object Logger extends Serializable{
  @transient lazy val log = LogManager.getLogger(classOf[LaunchETLExecution])
  log.setLevel(Level.INFO)
  log.setAdditivity(false)
  val ca = new ConsoleAppender()
  ca.setWriter(new OutputStreamWriter(System.out))
  ca.setLayout(new PatternLayout("%d{yyyy/MM/dd HH:mm:ss} %p %c{1}: %m%n"))
  ca.setName("myconsole")
  log.addAppender(ca)
}

