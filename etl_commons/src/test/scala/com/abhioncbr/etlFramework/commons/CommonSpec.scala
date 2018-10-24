package com.abhioncbr.etlFramework.commons

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.HADOOP_CONF
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class CommonSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()

    val dir: String = System.getProperty("user.dir")
    System.setProperty("hadoop.home.dir", dir)
    Context.addContextualObject[Configuration](HADOOP_CONF, new Configuration())
  }

}
