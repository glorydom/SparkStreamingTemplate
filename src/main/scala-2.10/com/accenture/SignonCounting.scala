package com.accenture

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._

import scala.util.parsing.json.JSON

/**
  * Created by huihui.b.zhang on 4/1/2016.
  */
object SignonCounting {

  def main(args:Array[String]): Unit ={
    if (args.length < 4) {
      System.err.println("Usage: SignonCounting <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    println("the parameters are: " + zkQuorum +" "+group + " "+ topics + " "+ numThreads +" ")
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

//    val lines = ssc.socketTextStream("192.168.56.101", 9999);
    val message = "{\"EnterpriseID\":\"claudia.a.barbosa@accenture.com\",\"PeopleKey\":\"1151508\",\"Level\":\"10\",\"Org\":\"Delivery Performance\",\"GeographicUnitCode\":\"USA\",\"GeographicUnit\":\"United States\",\"CountryCode\":\"US\",\"CountryCodeDescription\":\"USA\",\"CareerTrackDescription\":\"Client Delivery & Operations\",\"RelyingPartyIdentifier\":\"https://mmi.accenture.com/\",\"IdentityProviderIdentifier\":\"urn:federation:accenture\",\"AuthenticationMethod\":\"http://schemas.microsoft.com/ws/2008/06/identity/authenticationmethod/windows\",\"MFAEnabled\":\"false\",\"TokenIssued\":\"true\",\"ClientIPAddress\":\"0\",\"ClientUserAgent\":\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; MS-RTC LM 8; InfoPath.3; managedpc)\",\"IsRegistered\":\"false\",\"IsManaged\":\"false\",\"CompanyCode\":\"1000\",\"AuthorizationUTCTimestamp\":\"2015-11-12T19:07:05.456Z\",\"IssuedUTCTimestamp\":\"2015-11-12T19:07:05.612Z\"}"

    val signons:DStream[(Any, Int)] = lines.map( JSON.parseFull ).map( _ match {
      case Some(map: Map[String, Any]) => ((map("EnterpriseID"), map("RelyingPartyIdentifier"), map("IssuedUTCTimestamp")), 1)
      case None => println("Parsing failed"); (("", "", ""), 1)
      case other =>  println("Unknown data structure: " + other); (("", "", ""), 1)
    })

    val sumSignon = signons.reduceByKey(_ + _)

//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L))
//      val sdf =
//        wordCounts.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//    wordCounts.print()

    sumSignon.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
