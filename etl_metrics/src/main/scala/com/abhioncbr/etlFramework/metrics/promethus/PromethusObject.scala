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

package com.abhioncbr.etlFramework.metrics.promethus

import com.typesafe.scalalogging.Logger
import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.{CollectorRegistry, Gauge}

import scala.util.{Failure, Success, Try}

class PromethusObject(feedName: String) {
  private val logger = Logger(this.getClass)

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
      case Failure(th: Throwable) => logger.info(s"Unable to push metrics. Got an exception ${th.getStackTrace} ")
    }
  }

}
