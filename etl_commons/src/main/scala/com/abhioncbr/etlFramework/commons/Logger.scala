package com.abhioncbr.etlFramework.commons

import java.io.OutputStreamWriter

import com.abhioncbr.etlFramework.commons.job.ETLJob
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

//TODO: rewrite complete logging logic. for refactoring purpose it's present.
object Logger extends Serializable{
  @transient lazy val log = LogManager.getLogger(classOf[ETLJob])
  log.setLevel(Level.INFO)
  log.setAdditivity(false)
  val ca = new ConsoleAppender()
  ca.setWriter(new OutputStreamWriter(System.out))
  ca.setLayout(new PatternLayout("%d{yyyy/MM/dd HH:mm:ss} %p %c{1}: %m%n"))
  ca.setName("myconsole")
  log.addAppender(ca)
}

