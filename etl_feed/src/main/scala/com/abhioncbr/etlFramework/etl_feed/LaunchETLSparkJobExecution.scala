package com.abhioncbr.etlFramework.etl_feed

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType}
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.load.Load
import com.abhioncbr.etlFramework.commons.transform.Transform
import com.abhioncbr.etlFramework.etl_feed.extractData.{ExtractDataFromDB, ExtractDataFromHive, ExtractDataFromJson}
import com.abhioncbr.etlFramework.etl_feed.loadData.LoadDataIntoHiveTable
import com.google.common.base.Objects
import com.abhioncbr.etlFramework.etl_feed.transformData.{TransformData, ValidateTransformedDataSchema}
import com.abhioncbr.etlFramework.job_conf.xml.ParseETLJobXml
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}
import scala.xml.XML

class LaunchETLSparkJobExecution(venture: String, firstDate: DateTime, secondDate: DateTime, xmlInputFilePath: String){
  def configureFeedJob: Either[Boolean, String] ={
    Context.addContextualObject[String](VENTURE, venture)
    Context.addContextualObject[DateTime](FIRST_DATE, firstDate)
    Context.addContextualObject[DateTime](SECOND_DATE, secondDate)

    val sparkContext = new SparkContext(new SparkConf)
    Context.addContextualObject[Configuration](HADOOP_CONF, new Configuration)
    Context.addContextualObject[SparkContext](SPARK_CONTEXT, sparkContext)
    Context.addContextualObject[SQLContext](SQL_CONTEXT, new SQLContext(sparkContext))
    Context.addContextualObject[HiveContext](HIVE_CONTEXT, new HiveContext(sparkContext))

    val FALSE = false
    val parse = new ParseETLJobXml
    parse.parseXml(xmlInputFilePath, FALSE) match {
      case Left(xmlContent) => parse.parseNode(XML.loadString(xmlContent)) match {
        case Left(tuple) => Context.addContextualObject[JobStaticParam] (JOB_STATIC_PARAM, tuple._1)
          Context.addContextualObject[Extract] (EXTRACT, tuple._2)
          Context.addContextualObject[Transform] (TRANSFORM, tuple._3)
          Context.addContextualObject[Load] (LOAD, tuple._4)
        case Right(parseError) => Logger.log.error(parseError)
          return Right(parseError)
      }
      case Right(xmlLoadError) => Logger.log.error(xmlLoadError)
        return Right(xmlLoadError)
    }
    Left(true)
  }

  def executeFeedJob: Either[Array[JobResult], String]={
    val extractionResult = extract
    if(extractionResult.isRight) return Right(extractionResult.right.get)
    Logger.log.info("Extraction phase of the feed is completed")

    val transformedResult = transformation(extractionResult.left.get)
    if(transformedResult.isRight) return Right(transformedResult.right.get)
    Logger.log.info("Transformation phase of the feed is completed")

    val validateResult = validate(transformedResult.left.get)
    if(validateResult.isRight) return Right(validateResult.right.get)
    Logger.log.info ("Validation phase of the feed is completed")

    val loadResult = load(validateResult.left.get)
    if(loadResult.isRight) return Right(validateResult.right.get)
    Logger.log.info ("Load phase of the feed is completed")

    Left(loadResult.left.get)
  }

  private def extract: Either[DataFrame, String] = {
    val extractionType = Context.getContextualObject[Extract](EXTRACT).extractionType
    extractionType match {
      case ExtractionType.JSON => Left((new ExtractDataFromJson).getRawData(firstDate, Some(secondDate)))
      case ExtractionType.JDBC => Left((new ExtractDataFromDB).getRawData(firstDate, Some(secondDate)))
      case ExtractionType.HIVE => Left((new ExtractDataFromHive).getRawData(firstDate, Some(secondDate)))
      case _ => Right(s"extracting data from $extractionType is not supported right now.")
    }
  }

  private def transformation(extractionDF: DataFrame): Either[Array[ (DataFrame, Any, Any) ], String] = {
    //testing whether extracted data frame is having data or not. If not then, error message is returned.
    if(extractionDF.first == null) return Right("Extracted data frame contains no data row")

    new TransformData(Context.getContextualObject[Transform](TRANSFORM)).performTransformation(extractionDF)
  }

  private def validate(transformationDF: Array[ (DataFrame, Any, Any) ]): Either[Array[(DataFrame, DataFrame, Any, Any)], String] = {
    //testing whether transformed data frames have data or not.
    //As per Sreekanth, if one data frame doesn't have data, we will skip all data frames storage.
    transformationDF.unzip3._1.toArray.foreach(df => if(df.first == null ) return Right("Transformed data frame contains no data row"))

    var output: Array[ (DataFrame, DataFrame, Any, Any) ] = Array()
    transformationDF.foreach( arrayElement =>  {
      val validator = new ValidateTransformedDataSchema
      val validateSchemaResult = validator.validateSchema (arrayElement._1)
      validateSchemaResult._1 match {
        case true => output =  output ++ validator.validateData(arrayElement._1, validateSchemaResult._2.get, arrayElement._2, arrayElement._3)
        case false => validateSchemaResult._2 match {
          case Some(_2) => Logger.log.error("hive table schema & data frame schema does not match. Below are schemas for reference -")
            Logger.log.error(s"table schema:: ${_2.mkString}")
            Logger.log.error(s"data frame schema:: ${validateSchemaResult._3.get.mkString}")
          case None => Logger.log.error(s"provided hive table does not exist.")}
          return Right("Validation failed. Please check the log.")
      }
    })
    Left(output)
  }

  private def load(validationArrayDF: Array[ (DataFrame, DataFrame, Any, Any) ]): Either[Array[JobResult], String] = {
    //testing whether validated data frames have data or not.
    validationArrayDF.foreach(tuple => {
      if (tuple._1.first == null) {
        println(s"validate failed"); return Right("Transformed data frame contains no data row")
      }
    })

    val FALSE = false
    //TODO: load tables for multiple data frames
    val loadResult: Array[JobResult] = validationArrayDF.map( validate => {
      (( new LoadDataIntoHiveTable).loadTransformedData(validate._1, firstDate), validate._1.count(), validate._2.count(), validate._3, validate._4 ) }
    ).map(loadResult => {
      if(loadResult._1.isRight) JobResult(FALSE, "", loadResult._4.asInstanceOf[Int], loadResult._5.asInstanceOf[Int], loadResult._2, loadResult._3, loadResult._1.right.get)
      else JobResult(loadResult._1.left.get, "", loadResult._4.asInstanceOf[Int], loadResult._5.asInstanceOf[Int], loadResult._2, loadResult._3, "")})
    Left(loadResult)
  }

  def updateFeedStat(statScriptPath: String, jobSubtask: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim
    val subTask = if (jobSubtask.isEmpty) jobStaticParam.feedName else jobSubtask

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.feedName} $subTask $venture $status ${jobStaticParam.processFrequency.toString.toLowerCase}
          |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
          |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    Logger.log.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }
}

object LaunchETLSparkJobExecution extends App{

  def launch(args: Array[String]): Unit = {
    case class CommandOptions(venture: String = "", firstDate: DateTime = null, feedName : String = "",
                              secondDate: DateTime = null, xmlInputFilePath: String = "", statScriptFilePath: String = "") {
      override def toString =
        Objects.toStringHelper(this)
          .add("venture", venture)
          .add("feed_name", feedName)
          .add("firstDate", firstDate)
          .add("secondDate", secondDate)
          .add("xmlFilePath", xmlInputFilePath)
          .add("statScriptFilePath", statScriptFilePath)
          .toString
    }
    val DatePattern = "yyyy-MM-dd HH:mm:ss"
    val dateParser  = DateTimeFormat.forPattern(DatePattern)

    val parser = new scopt.OptionParser[CommandOptions]("") {
      opt[String]('v', "venture")
        .action((v, c) => c.copy(venture = v))
        .text("required venture for which job need to be executed.")
        .required

      opt[String]('e', "etl_feed_name")
        .action((e, f) => f.copy(feedName = e))
        .text("required Etl feed name.")
        .required

      opt[String]('d', "date")
        .action((fd, c) =>  c.copy(firstDate = if(!fd.trim.isEmpty) dateParser.parseDateTime(fd) else DateTime.now.minusHours(1)))
        .text("required for all jobs except of hourly jobs.")
        .optional

      opt[String]('s', "second_date")
        .action((sd, c) =>  c.copy(secondDate = if(!sd.trim.isEmpty) dateParser.parseDateTime(sd) else null))
        .text("required only in case of date range jobs")
        .optional

      opt[String]('x', "xml_file_path")
        .action((xfp, c) => c.copy(xmlInputFilePath = xfp))
        .text("required xml file path for etl job execution steps.")
        .required

      opt[String]('f', "stat_script_file_path")
        .action((sfp, c) => c.copy(statScriptFilePath = sfp))
        .text("required stat script file path for etl job result updates.")
        .required

    }

    parser.parse(args, CommandOptions()) match {
      case Some(opts) =>
        Logger.log.info(s"Going to start the execution of the etl feed job with following params: $opts")
        var exitCode = -1
        val etlExecutor = new LaunchETLSparkJobExecution(opts.venture, opts.firstDate, opts.secondDate, opts.xmlInputFilePath)

        @transient lazy val feedDataStatGauge = Gauge.build()
          .name(opts.feedName.replace("-","_"))
          .help(s"number of entries for a given ${opts.feedName.replace("-","_")}")
          .labelNames("venture", "service")
          .register()

        var metricData = 0L
        etlExecutor.configureFeedJob match {
          case Left(b) => val start = System.currentTimeMillis()
            val feedJobOutput = etlExecutor.executeFeedJob
            val end = System.currentTimeMillis()
            feedJobOutput match {
              case Left(dataArray) => val temp = dataArray.map(data =>
                etlExecutor.updateFeedStat(opts.statScriptFilePath, data.subtask, data.validateCount,
                  data.nonValidatedCount, end - start, "success", data.transformationPassedCount,
                  data.transformationFailedCount, data.failureReason)).map(status => if(status==0) true else false)
                .reduce(_ && _)
                if(temp) exitCode = 0
                metricData = dataArray.head.validateCount
              //else
              case Right(s) => etlExecutor.updateFeedStat(opts.statScriptFilePath, "", 0, 0, end - start, "fail", 0, 0, s)
                Logger.log.error(s)
            }
          case Right(s) => etlExecutor.updateFeedStat(opts.statScriptFilePath, "", 0, 0, 0, "fail", 0, 0, s)
            Logger.log.error(s)
        }

        feedDataStatGauge.labels(opts.venture, opts.feedName).set(metricData)
        pushMetrics(opts.feedName)

        Logger.log.info(s"Etl job finish with exit code: $exitCode")
        System.exit(exitCode)
      case None =>
        parser.showTryHelp()
    }
  }

  def pushMetrics(MetricsJobName: String): Unit = {
    @transient val conf: Map[String, String] = Map()
    val pushGatewayAddress = conf.getOrElse("pushGatewayAddr", "sgdshadoopedge3.sgdc:9091")
    val pushGateway = new PushGateway(pushGatewayAddress)

    Try(pushGateway.push(CollectorRegistry.defaultRegistry, s"${MetricsJobName.replace("-","_")}")) match {
      case Success(u: Unit) => Logger.log.info("Metrics pushed.")
                               true
      case Failure(th: Throwable) => Logger.log.info(s"Unable to push metrics. Got an exception ${th.getStackTrace} ")
    }
  }

  launch(args)
}