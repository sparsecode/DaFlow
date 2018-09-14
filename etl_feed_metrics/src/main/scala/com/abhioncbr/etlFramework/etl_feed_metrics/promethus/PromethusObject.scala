package com.abhioncbr.etlFramework.etl_feed_metrics.promethus

import com.abhioncbr.etlFramework.commons.Logger
import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.{CollectorRegistry, Gauge}

import scala.util.{Failure, Success, Try}

class PromethusObject(feedName: String) {

  @transient lazy val feedDataStatGauge = Gauge.build()
    .name(feedName.replace("-","_"))
    .help(s"number of entries for a given ${feedName.replace("-","_")}")
    .register()

  def pushMetrics(MetricsJobName: String, metricData: Long): Unit = {
    @transient val conf: Map[String, String] = Map()
    val pushGatewayAddress = conf.getOrElse("pushGatewayAddr", "sgdshadoopedge3.sgdc:9091")
    val pushGateway = new PushGateway(pushGatewayAddress)

    feedDataStatGauge.labels(feedName).set(metricData)

    Try(pushGateway.push(CollectorRegistry.defaultRegistry, s"${MetricsJobName.replace("-","_")}")) match {
      case Success(u: Unit) => true
      case Failure(th: Throwable) => Logger.log.info(s"Unable to push metrics. Got an exception ${th.getStackTrace} ")
    }
  }

}
