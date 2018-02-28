package com.lzd.etlFramework.etl.feed

import java.text.DecimalFormat

import com.google.common.base.Objects
import com.lzd.etlFramework.etl.feed.common.ContextConstantEnum._
import com.lzd.etlFramework.etl.feed.extractData.{ExtractDataFromDB, ExtractDataFromHive, ExtractDataFromJson, ExtractionType}
import com.lzd.etlFramework.etl.feed.loadData.LoadDataIntoHiveTable
import com.lzd.etlFramework.etl.feed.transformData.{TransformData, ValidateTransformedDataSchema}
import com.lzd.etlFramework.etl.feed.common._
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}
import scala.xml.XML

class LaunchETLExecution(spark_master_mode : String, feed_name: String, job_queue: String, job_jars: Array[String],
                         venture: String, firstDate: DateTime, secondDate: DateTime, xmlInputFilePath: String,
                         driverMemory: String, executorMemory: String){

  def configureFeedJob: Either[Boolean, String] ={
    Context.addContextualObject[String](VENTURE, venture)
    Context.addContextualObject[DateTime](FIRST_DATE, firstDate)
    Context.addContextualObject[DateTime](SECOND_DATE, secondDate)

    val sparkContext = new SparkContext(new SparkConf()
      .setAppName(feed_name)
      .setMaster(spark_master_mode)
      .set("spark.yarn.queue", job_queue)
      .set("spark.driver.memory", driverMemory)
      .set("spark.executor.memory", executorMemory)
      .set("spark.executor.instances", "3")
      .setJars(job_jars.toSeq)
//    .setJars(Seq("/usr/local/airflow/dags/etl_feed-assembly-0.1.0.jar", "/user/abhishesharma/etl/etl_feed-assembly-0.1.0.jar", "/mnt/hadoop/data-science.lazada.sg/user/abhishesharma/etl/etl_feed-assembly-0.1.0.jar"))
      .set("spark.sql.hive.metastore.sharedPrefixes", "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity,com.mapr.fs.jni")
      )

    //setting log level for spark
    sparkContext.setLogLevel("WARN")

    Context.addContextualObject[Configuration](HADOOP_CONF, new Configuration)
    Context.addContextualObject[SparkContext](SPARK_CONTEXT, sparkContext)
    Context.addContextualObject[SQLContext](SQL_CONTEXT, new SQLContext(sparkContext))
    Context.addContextualObject[HiveContext](HIVE_CONTEXT, new HiveContext(sparkContext))

    val TRUE = true
    val parse = new ParseETLJobXml
    parse.parseXml(xmlInputFilePath, TRUE) match {
      case Left(xmlContent) => parse.parseNode(XML.loadString(xmlContent)) match {
        case Left(tuple) => Context.addContextualObject[JobStaticParam] (JOB_STATIC_PARAM, tuple._1)
          Context.addContextualObject[Extract] (EXTRACT, tuple._2)
          Context.addContextualObject[Transform] (TRANSFORM, tuple._3)
          Context.addContextualObject[Load] (LOAD, tuple._4)
        case Right(parseError) => return Right(parseError)
      }
      case Right(xmlLoadError) => return Right(xmlLoadError)
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

  //function for updating etl_feed_stat table through shell script. For airflow-docker image usage, we are going to use other function.
  @deprecated
  def updateFeedStat(statScriptPath: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.feedName} $feed_name $venture $status ${jobStaticParam.processFrequency.toString.toLowerCase}
         |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
         |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    Logger.log.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }

  def updateFeedStat(hiveDbName: String, tableName: String, pathInitial: String,
                     validated: Long, nonValidated: Long, executionTime: Long,
                      status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)

    val reason = if (failureReason.isEmpty) "None" else failureReason.trim

    val sc: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    val rdd = sc.parallelize(Seq(Seq(feed_name, status, jobStaticParam.processFrequency.toString.toLowerCase, new java.sql.Date(firstDate.getMillis), new DecimalFormat("00").format(firstDate.getHourOfDay),
                                          reason, transformed , nonTransformed, validated, nonValidated, executionTime, jobStaticParam.feedName, venture)))
    val rowRdd = rdd.map(v => org.apache.spark.sql.Row(v: _*))

    val hiveContext: HiveContext = Context.getContextualObject[HiveContext](HIVE_CONTEXT)
    val statDF = hiveContext.sql(s"""select * from $hiveDbName.$tableName where venture='$venture' and job_name='${jobStaticParam.feedName}'""".stripMargin)

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val newRow = sqlContext.createDataFrame(rowRdd, statDF.schema)
    val updatedStatDF = statDF.unionAll(newRow)

    val path = s"$pathInitial/$hiveDbName/$tableName/${jobStaticParam.feedName}/$venture"
    val tmpPath = path + "_tmp"

    try{
      println(s"Writing updated stats in to the table 'etl_feed_stat' to HDFS ($path)")
      updatedStatDF.write.mode(SaveMode.Overwrite).parquet(tmpPath)

      val hadoopFs = FileSystem.get(new Configuration())
      Try(hadoopFs.delete(new Path(path + "_bkp"), true))
      Try(hadoopFs.rename(new Path(path), new Path(path + "_bkp")))
      hadoopFs.rename(new Path(tmpPath), new Path(path))

      hiveContext.sql(
        s"""
           |ALTER TABLE $hiveDbName.$tableName
           | ADD IF NOT EXISTS PARTITION (job_name ='${jobStaticParam.feedName}' , venture = '$venture' )
           | LOCATION '$path'
         """.stripMargin)

      println(s"updated stats partition at ($path) registered successfully to $hiveDbName.$tableName")
      0
    } finally -1
  }
}

object LaunchETLExecution extends App{

  def launch(args: Array[String]): Unit = {
    case class CommandOptions(spark_master_mode: String = "", queue: String ="", jars: Array[String] = null, venture: String = "", feedName : String = "",
                              firstDate: DateTime = null, secondDate: DateTime = null, xmlInputFilePath: String = ""
                              //, statScriptFilePath: String = ""
                              , executorMemory: String = "", driverMemory: String = ""
                              , statParams: Array[String] = null) {
      override def toString =
        Objects.toStringHelper(this)
          .add("spark_master_mode", spark_master_mode)
          .add("feed_name", feedName)
          .add("job_queue", queue)
          .add("job_jars", jars.toString)
          .add("venture", venture)
          .add("firstDate", firstDate)
          .add("secondDate", secondDate)
          .add("xmlFilePath", xmlInputFilePath)
          .add("stat_params", statParams.toString)
          .add("executorMemory", executorMemory)
          .add("driverMemory", driverMemory)
          .toString
    }
    val DatePattern = "yyyy-MM-dd HH:mm:ss"
    val dateParser  = DateTimeFormat.forPattern(DatePattern)

    val parser = new scopt.OptionParser[CommandOptions]("") {
      opt[String]('q', "job_queue")
        .action((q, n) => n.copy(queue = q))
        .text("required 'job queue' name. for eg. 'adhoc' or 'datapipes'")
        .required

      opt[String]('e', "etl_feed_name")
        .action((e, f) => f.copy(feedName = e))
        .text("required Etl feed name.")
        .required


      opt[String]('m', "spark_master_mode")
        .action((v, m) => m.copy(spark_master_mode = v))
        .text("required for launching spark job.")
        .required

      opt[String]('j', "job_jars")
        .action((j, m) => m.copy(jars = j.split(",").map(str => str.trim)))
        .text("Additional jars needed for spark job.")
        .required

      opt[String]('v', "venture")
        .action((v, c) => c.copy(venture = v))
        .text("required venture for which job need to be executed.")
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

      opt[String]('t', "executor_memory")
        .action((em, iem) => iem.copy(executorMemory = em))
        .text("required executor memory for etl job")
        .required

      opt[String]('r', "driver_memory")
        .action((dm, idm) => idm.copy(driverMemory = dm))
        .text("required driver memory for etl job")
        .required

      /*opt[String]('f', "stat_script_file_path")
        .action((sfp, c) => c.copy(statScriptFilePath = sfp))
        .text("required stat script file path for etl job result updates.")
        .required*/

      opt[String]('p', "stat_params")
        .action((sp, m) => m.copy(statParams = sp.split(",").map(str => str.trim)))
        .text("params for updating etl feed stats i.e dbName, tableName, fileInitialPath")
        .required
    }

    parser.parse(args, CommandOptions()) match {
      case Some(opts) =>
        Logger.log.info(s"Going to start the execution of the etl feed job with following params: $opts")
        var exitCode = -1
        val etlExecutor = new LaunchETLExecution(opts.spark_master_mode, opts.feedName, opts.queue, opts.jars, opts.venture,
                                                  opts.firstDate, opts.secondDate, opts.xmlInputFilePath, opts.driverMemory, opts.executorMemory)

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
                                                      etlExecutor.updateFeedStat(opts.statParams(0), opts.statParams(1), opts.statParams(2),
                                                        data.validateCount, data.nonValidatedCount, end - start, "success",
                                                        data.transformationPassedCount, data.transformationFailedCount, data.failureReason)).map(status => if(status==0) true else false)
                                                      .reduce(_ && _)
                                                      if(temp) exitCode = 0
                                metricData = dataArray.head.validateCount

                              //else
                              case Right(s) => etlExecutor.updateFeedStat(opts.statParams(0), opts.statParams(1), opts.statParams(2), 0, 0, end - start, "fail", 0, 0, s)
                                               Logger.log.error(s)
                            }
            case Right(s) => etlExecutor.updateFeedStat(opts.statParams(0), opts.statParams(1), opts.statParams(2), 0, 0, 0, "fail", 0, 0, s)
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
      case Success(u: Unit) => true
      case Failure(th: Throwable) => Logger.log.info(s"Unable to push metrics. Got an exception ${th.getStackTrace} ")
    }
  }

  //for checking the class path and environment variables, uncomment it.
  /* val cl = ClassLoader.getSystemClassLoader
  cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)

  val environmentVars = System.getenv()
  import collection.JavaConversions._
  for ((k,v) <- environmentVars) println(s"key: $k, value: $v")*/

  launch(args)
}