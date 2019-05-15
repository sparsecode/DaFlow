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

package com.abhioncbr.daflow.metrics.promethus

import com.abhioncbr.daflow.commons.NotificationMessages
import com.typesafe.scalalogging.Logger
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.PushGateway
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class PrometheusObject(feedName: String, pushGatewayIpAddress: String) {
  private val logger = Logger(this.getClass)

  @transient lazy val feedDataStatGauge: Gauge = Gauge.build()
    .name(feedName).help(s"number of entries for a given $feedName").register()

  def pushMetrics(metricsJobName: String, metricData: Long): Either[Unit, String] = {
    @transient val conf: Map[String, String] = Map()
    val pushGatewayAddress = conf.getOrElse("pushGatewayAddr", pushGatewayIpAddress)
    val pushGateway = new PushGateway(pushGatewayAddress)

    feedDataStatGauge.labels(feedName).set(metricData)

    val output: Either[Unit, String] = Try(pushGateway.push(CollectorRegistry.defaultRegistry, metricsJobName)) match {
      case Success(u: Unit) => Left(u)
      case Failure(ex: Exception) => val str = s"Unable to push metrics. ${NotificationMessages.exceptionMessage(ex)}"
        logger.warn(str)
        Right(str)
    }
    output
  }

}
