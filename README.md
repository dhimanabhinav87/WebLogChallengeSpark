# WebLogChallengeSpark
Apache WebLogChallenge data analysis using Spark
This is a data pipe line  implementation in Cloudera Hadoop using Spark, Hive, Shell and HDFS.

1) WebLogChallenge.scala : Spark implementation for parsing weblogs given in paytm_sample.log. These are paytm webserver logs. These are the tasks that are done by this script. 
  Part 1 : Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
  Part 2: Determine the average session time
  Part 3: Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
  Part 4: Find the most engaged users, ie the IPs with the longest session times
  Session window time is chosen to be  15 minutes.
  Please update the values of the variables as per your implementation
  
2) WebLogChallengeBonus.scala: This script takes into account combination of userIp and userAgent for sessionizing the log.Apart from this everthing is done same as WebLogChallenge.scala. Please update the values of the variables as per your implementation
  

3) hive_action :This script creates hive external tables after processing the data from 1 and 2. In Cloudera, you can also use Impala for faster querying to see and analyze results. Please update the name of the tables as per your implementation
  

4) paytm_sample.log: The data file. The log file was taken from an AWS Elastic Load Balancer: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format

5) pom.xml: Maven dependency file
