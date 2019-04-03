// Databricks notebook source
// MAGIC %sql
// MAGIC Drop table if exists diamonds

// COMMAND ----------

// MAGIC %sql
// MAGIC create TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY price

// COMMAND ----------

val friendsDF = spark.sql("select age, num_friends from fakefriends_csv")
friendsDF.show()

// COMMAND ----------

display(friendsDF)

// COMMAND ----------

val averageByAgeDF = friendsDF.groupBy("age").avg("num_friends").orderBy("age")
display(averageByAgeDF)


// COMMAND ----------

val friendsRdd = friendsDF.rdd
val friendsPairRdd =  friendsRdd.map(x => (x.getInt(0), x.getInt(1)))
val friendsTotalByAgeRdd = friendsPairRdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
val friendsAvgByAgeRdd = friendsTotalByAgeRdd.mapValues( x => x._1.toDouble/x._2)
val results = friendsAvgByAgeRdd.collect()
results.sorted.foreach(println)

                         

// COMMAND ----------

import org.apache.spark.sql.types._

val inputPath = "/databricks-datasets/structured-streaming/events/"
val jsonSchema =  new StructType().add("time", "timestamp").add("action", "string")
val streamingInputDF  = spark
    .readStream
    .schema(jsonSchema)               
    .option("maxFilesPerTrigger", 1) 
    .json(inputPath)
streamingInputDF.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._
val streamingCountsDF  = streamingInputDF
    .groupBy(
      window($"time", "1 hour"),
      $"action")
    .count()



//display(streamingCountsDF)


streamingCountsDF
    .writeStream
    .format("memory")        
    .queryName("counts")     
    .outputMode("complete")  
    .start()


/*println("Number of Partitions ...=> " + streamingCountsDF.rdd)*/


val eventHubSchemaCompliantDF = streamingCountsDF.select($"action").withColumnRenamed("action", "body")


import org.apache.spark.eventhubs.{ ConnectionStringBuilder,EventHubsConf, EventPosition }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val connectionString = ConnectionStringBuilder()
  .setNamespaceName("sauraveventhub")
  .setEventHubName("sauravevent")
  .setSasKeyName("eventhubAdminPolicy")
  .setSasKey(dbutils.secrets.get(scope = "my-azure-keyvault-scope", key = "EventhubSASKey"))
  .build
  //.setSasKey("ee1Z6k/c4YYytwyyMqDyzoLLptqt4VDpiqto/4NXayI=")

 
val eventHubsConfWrite = EventHubsConf(connectionString)

//println(eventHubsConf.toMap)

val query =
  eventHubSchemaCompliantDF
    .writeStream
    .format("eventhubs")
    .queryName("eventHubcounts")
    .outputMode("complete")
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("25 seconds"))
    .option("checkpointLocation", "/mycheckpoint/")
    .start()

// COMMAND ----------

val test = "Hello"
dbutils.secrets.get(scope = "my-azure-keyvault-scope", key = "EventhubSASKey")

dbutils.fs.mkdirs("/mnt/mybucket")

// COMMAND ----------

// MAGIC %md #Setting ADL2 Connectivity

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adl2storageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adl2storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adl2storageaccount.dfs.core.windows.net", "b4703129-ed69-4233-bd4d-175e7aa8e00d")
spark.conf.set("fs.azure.account.oauth2.client.secret.adl2storageaccount.dfs.core.windows.net", "xW6ZCR7kOZOfyBqSEithbYnbjRKvJ2/x4qUEM0LkdLI=")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adl2storageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/c18be700-2da6-4389-8212-e7f0d23cdf70/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://adl2fs@adl2storageaccount.dfs.core.windows.net/mybucket")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")




// COMMAND ----------



/*val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "5254fdd7-2779-49e2-984e-926288fd4604",
  "fs.azure.account.oauth2.client.secret" -> "9//W0jnHF+4GVe66FzkcmwkiLegyPzadgUMXQOY39G4=",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/d31b6887-7485-4edf-9db9-c352174b137b/oauth2/token")
  */

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "b4703129-ed69-4233-bd4d-175e7aa8e00d",
  "fs.azure.account.oauth2.client.secret" -> "xW6ZCR7kOZOfyBqSEithbYnbjRKvJ2/x4qUEM0LkdLI=",
   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/c18be700-2da6-4389-8212-e7f0d23cdf70/oauth2/token")



// Optionally, you can add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://adl2fs@adl2storageaccount.dfs.core.windows.net/mybucket",
  mountPoint = dis",
  extraConfigs = configs)

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
val connectionString = ConnectionStringBuilder()
  .setNamespaceName("sauraveventhub")
  .setEventHubName("sauravevent")
  .setSasKeyName("eventhubAdminPolicy")
  .setSasKey("ee1Z6k/c4YYytwyyMqDyzoLLptqt4VDpiqto/4NXayI=")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromStartOfStream)
  
val eventhubsRead = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()


 display(eventhubsRead.select($"body".cast("string")))

 /*eventhubsRead
    .writeStream
    .format("memory")        
    .queryName("eventhubsRead")     
    .outputMode("append")  
    .start()*/

// COMMAND ----------

// MAGIC %md #Databricks Delta

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val events = spark.read 
  .option("inferSchema", "true") 
  .json("/databricks-datasets/structured-streaming/events/") 
  .withColumn("date", expr("time"))
  .drop("time")
  .withColumn("date", from_unixtime($"date", "yyyy-MM-dd"))
  
display(events)

// COMMAND ----------

import org.apache.spark.sql.SaveMode

events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events/")

// COMMAND ----------

import org.apache.spark.sql.SaveMode

events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events/")

// COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS events"))
        
display(spark.sql("CREATE TABLE events_delta USING DELTA LOCATION '/delta/events/'"))

// COMMAND ----------

val events_delta = spark.sql("select * from events_delta")
display(events_delta)

// COMMAND ----------

events_delta.count()

// COMMAND ----------

historical_events = spark.read 
  .option("inferSchema", "true") 
  .json("/databricks-datasets/structured-streaming/events/") 
  .withColumn("date", expr("time-172800"))
  .drop("time") 
  .withColumn("date", from_unixtime("date", "yyyy-MM-dd"))


// COMMAND ----------



/*historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events*/



// COMMAND ----------


display(spark.sql("DESCRIBE HISTORY events_delta"))

// COMMAND ----------

dbutils.fs.ls("dbfs:/delta/events")

// COMMAND ----------

display(spark.sql("DESCRIBE DETAIL events_delta"))

// COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS events_delta1"))
        
display(spark.sql("CREATE TABLE events_delta1 (action STRING,date STRING) USING DELTA PARTITIONED BY (date) LOCATION '/delta/events_delta1/'"))

//display(spark.sql("CREATE TABLE events_delta1 (action STRING,date STRING) USING DELTA LOCATION '/delta/events_delta1/'"))

// COMMAND ----------

display(spark.sql("DESCRIBE FORMATTED events_delta"))

// COMMAND ----------

events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events_delta1/")

// COMMAND ----------

display(spark.sql("DESCRIBE DETAIL events_delta1"))

// COMMAND ----------

dbutils.fs.ls("dbfs:/delta/events_delta1")

// COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS events_delta2"))
        
display(spark.sql("CREATE TABLE events_delta2 (action STRING,date STRING) USING DELTA PARTITIONED BY (date)"))
//display(spark.sql("CREATE TABLE events_delta2 (action STRING,date STRING) USING DELTA"))

// COMMAND ----------

dbutils.fs.ls("dbfs:/delta/")

// COMMAND ----------

events.write.format("delta").mode("overwrite").saveAsTable("events_delta2")


// COMMAND ----------

display(spark.sql("DESCRIBE DETAIL events_delta2"))

// COMMAND ----------

display(spark.sql("DROP TABLE events_delta1"))

// COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/")

// COMMAND ----------

//dbutils.fs.help()
dbutils.fs.rm("dbfs:/delta/events_delta2/",true)



// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.widgets.dropdown("x1232133123", "1", [str(x) for x in range(1, 10)], "hello this is a widget 2")

// COMMAND ----------

dbutils.widgets.get("x1232133123")

// COMMAND ----------

// MAGIC %sql
// MAGIC select getArgument("x1232133123") from dual

// COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/mybucket")

// COMMAND ----------

// MAGIC %sh
// MAGIC wget --quiet -O /mnt/jars/driver-daemon/adal4j-1.6.0.jar http://central.maven.org/maven2/com/microsoft/azure/adal4j/1.6.0/adal4j-1.6.0.jar

// COMMAND ----------

dbutils.fs.mkdirs("/mnt/jars/driver-daemon")

// COMMAND ----------

dbutils.fs.ls("/mnt/driver-daemon/jars")

// COMMAND ----------

// MAGIC %sh
// MAGIC wget -O /mnt/driver-daemon/jars/adal4j-1.6.0.jar http://central.maven.org/maven2/com/microsoft/azure/adal4j/1.6.0/adal4j-1.6.0.jar

// COMMAND ----------

dbutils.fs.ls("/mnt/driver-daemon/jars/adal4j-1.6.0.jar")

// COMMAND ----------

dbutils.fs.ls("/FileStore/jars")

// COMMAND ----------

dbutils.fs.ls("/")

// COMMAND ----------

dbutils.fs.ls("/FileStore/")

// COMMAND ----------

// MAGIC %sh
// MAGIC wget --quiet -O /tmp/jars/ http://central.maven.org/maven2/com/microsoft/azure/adal4j/1.6.0/adal4j-1.6.0.jar

// COMMAND ----------

dbutils.fs.cp("/FileStore/jars/maven/com/microsoft/azure/adal4j-1.6.0.jar","/mnt/jars/driver-daemon")

// COMMAND ----------

dbutils.fs.ls("/mnt/jars/driver-daemon/")

// COMMAND ----------

Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------


val jdbcHostname = "sauravdbserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "sauravdb"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
val jdbcUsername = "saurav@sauravdbserver"
val jdbcPassword = "Govind123"
// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

val pushdown_query = "(select * from SYS.SYSOBJECTS where ID = 3) sysobj_alias"
val sysObjects = spark.read.jdbc(jdbcUrl, pushdown_query, connectionProperties)
display(sysObjects)

// COMMAND ----------

val pushdown_query = "(select * from SYS.SYSOBJECTS where ID = 3) sysobj_alias"
val df = spark.read.format("jdbc").
  option("url", "jdbc:sqlserver://sauravdbserver.database.windows.net:1433;database=sauravdb;").
  option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
  option("user", "sumanta@sauravdomain.onmicrosoft.com").
  option("password", "Haribol@123").
  option("authentication","ActiveDirectoryPassword").
  option("encrypt",false).
  option("dbtable",pushdown_query).
  load()


// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "sauravdbserver.database.windows.net",
  "databaseName"   -> "sauravdb",
  "dbTable"        -> "SYS.SYSOBJECTS",
  "user"           -> "sumanta@sauravdomain.onmicrosoft.com",
  "password"       -> "Haribol@123",
  "encrypt"        -> "true",
  "trustServerCertificate"  -> "true",
  //"hostNameInCertificate" -> "*.database.windows.net",
  "authentication"  -> "ActiveDirectoryPassword"
))

val collection = spark.read.sqlDB(config)
collection.show()

// COMMAND ----------

display(dbutils.fs.ls("/mnt/mybucket2"))

// COMMAND ----------

// MAGIC %md Hellow

// COMMAND ----------

