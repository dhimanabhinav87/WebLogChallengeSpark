package com.adhiman.spark

import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.control.Exception._
import java.util.regex.Matcher
import scala.util.{Try, Success, Failure}
import java.text.SimpleDateFormat
import java.util.Date

import com.adhiman.spark.MyKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object WebLogChallengeBonus {
  //case class for weblogDf schema
  case class WebLogDf(timestamp: String, userIp: String, url: String, userAgent:String, epoch:Long)
  //case class for the log file
  case class WebRecord(timestamp: String, elb: String, clientIp: String, backendIp:String ,request_processing_time: String, backend_processing_time: String
                       , response_processing_time:String, elb_status_code:String, backend_status_code:String, received_bytes:String
                       , sent_bytes:String, request:String, user_agent:String, ssl_cipher:String
                       , ssl_protocol:String)
  //2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
  val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  //converting the datetime to epoch date for calculating session durations
  def addEpochDate(date:String)= {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val dt = df.parse(date);
    val epoch = dt.getTime();
    (epoch)

  }
  //parsing each record in log file using the function
  def parseLogLine(log: String): WebRecord = {
    try {
      val res = PATTERN.findFirstMatchIn(log)

      if (res.isEmpty) {
        println("Rejected Log Line: " + log)
        WebRecord("Empty", "-", "-", "", "",  "", "", "-", "-","","","","","","" )
      }
      else {
        val m = res.get
        // NOTE:   HEAD does not have a content size.
        if (m.group(4).equals("-")) {
          WebRecord(m.group(1), m.group(2), m.group(3),"",
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
        else {
          WebRecord(m.group(1), m.group(2), m.group(3),m.group(4),
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
      }
    } catch
      {
        case e: Exception =>
          println("Exception on line:" + log + ":" + e.getMessage);
          WebRecord("Empty", "-", "-", "", "",  "", "", "-", "-","","","","","","" )
      }
  }





  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("spark table write process")  // DONE - master (local or YARN) is set while submitting the job
      .set("spark.hadoop.validateOutputSpecs", "false").set("spark.storage.memoryFraction", "1")
      .set("spark.sql.parquet.compression.codec","gzip")
    MyKryoRegistrator.register(sparkConf)
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true") //Enables reading directory recursively
    val blockSize = 1024 * 1024 * 256      // Same as HDFS block size
    /*Intializing the spark context with block size values*/
    sc.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
    sc.hadoopConfiguration.setInt( "parquet.block.size", blockSize )

    val sqlContext = new SQLContext(sc)
    val  hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val vconf = new Configuration()
    val fs = FileSystem.get(vconf)
    //for toDF
    import sqlContext.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    //converting the text file into RDD
    val loglines = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/paytm/data/2015_07_22_mktplace_shop_web_log_sample.log")
    //parsing each line of file and transforming it
    val parsedLogLines= loglines.map(parseLogLine)
    //filter any data that is not considered as session and only take timestamp, userIp, url,user_agent, epoch
    var filterLogLines=parsedLogLines.filter(logRecord=>logRecord.backendIp!="-" && logRecord.request_processing_time!="-1" && logRecord.backend_processing_time!="-1" &&
      logRecord.response_processing_time!="-1").map(logRecord=>(logRecord.timestamp,logRecord.clientIp,logRecord.request,logRecord.user_agent,addEpochDate(logRecord.timestamp)))
    //adding schema to log Rdd using Case Class WebLogDf
    val webLogWithSchema= filterLogLines.map(webLog=>WebLogDf(webLog._1,webLog._2,webLog._3,webLog._4,webLog._5))
    //converting RDD to Data Frame using hiveContext to make sure we can use Window Functions
    val webLogDf= hiveContext.createDataFrame(webLogWithSchema)
    //define windowing scheme for bonus part
    val windowSpec = Window.partitionBy($"userIp",$"userAgent").orderBy("epoch")

    //udf for deciding if new session began(15 mins)
    val isNewSession = udf((duration: Long) => {
      if (duration > 900000) 1
      else 0
    })

    //Define a udf to concatenate three passed in string values
    val getConcatenated = udf( (first: String, second: String, third: String) => { first + "_" + second+"_"+third } )
    //using window function getting the previous epoch using lag
    val webLogDfWithEpoch= webLogDf.withColumn("prevEpoch",lag(webLogDf("epoch"), 1).over(windowSpec))
    //cleaning epoch column by removing nulls
    val webLogDfWithEpochCleaned= webLogDfWithEpoch.withColumn("prevEpoch_cleaned", coalesce('prevEpoch, 'epoch))
    //calculating duration
    val webLogDfWithDuration= webLogDfWithEpochCleaned.withColumn("duration_miliseconds",webLogDfWithEpochCleaned("epoch")-webLogDfWithEpochCleaned("prevEpoch_cleaned"))
    //adding isNewSession column using a helper function isNewSession
    val webLogDfWithNewSessionFlag= webLogDfWithDuration.withColumn("isNewSession",isNewSession($"duration_miliseconds"))
    //adding window index column
    val webLogDfWithWindowIdx=webLogDfWithNewSessionFlag.withColumn("windowIdx",sum("isNewSession").over(windowSpec).cast("string"))
    //adding a new column SessionId by concatinating  index from Window function+ userIp
    val webLogDfWithSessionId=webLogDfWithWindowIdx.withColumn("sessionId",getConcatenated($"userIp",$"windowIdx",$"userAgent")).select($"userIp",$"sessionId",$"duration_miliseconds",$"url",$"userAgent").cache

    //Part 1 Sessionize the web log by IP and write to hdfs
    webLogDfWithSessionId.show()
    webLogDfWithSessionId.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save("hdfs://quickstart.cloudera:8020/user/cloudera/paytm/output/part1/")

    //Part 2 Determine the average session time and write to hdfs
    webLogDfWithSessionId.select(mean("duration_miliseconds")).show()
    webLogDfWithSessionId.select(mean("duration_miliseconds")).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save("hdfs://quickstart.cloudera:8020/user/cloudera/paytm/output/part2/")

    //Part 3 Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session and write to hdfs
    val urlVisitsPerSessionBase=webLogDfWithSessionId.select("sessionId","url").groupBy('sessionId).agg((collect_set("url")))
    val urlVisitsPerSession= urlVisitsPerSessionBase.select($"sessionId",size($"collect_set(url)"))
    urlVisitsPerSession.show()
    urlVisitsPerSession.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save("hdfs://quickstart.cloudera:8020/user/cloudera/paytm/output/part3/")

    //Part 4: Find the most engaged users, ie the IPs with the longest session times and write to hdfs
    webLogDfWithSessionId.groupBy("userIp").sum("duration_miliseconds").sort($"sum(duration_miliseconds)".desc).show()
    webLogDfWithSessionId.groupBy("userIp").sum("duration_miliseconds").sort($"sum(duration_miliseconds)".desc).repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").m

  }
}


