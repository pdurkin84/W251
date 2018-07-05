package org.apache.spark.examples.streaming.twitter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main extends App {
	println(s"I got executed with ${args size} args, they are: ${args mkString ", "}")

    System.setProperty("twitter4j.oauth.consumerKey", "lMHMMbmNxtlLHcZA9Bqxh8h6w")
    System.setProperty("twitter4j.oauth.consumerSecret", "KAT8bFoZknI1TYnx19Gx6p4cQqMbTtFAUOoBf1ndmHQ7eskgEX")
    System.setProperty("twitter4j.oauth.accessToken", "934581358344790016-yq3GrSkXlZoTM0Yskv9b2DoTNBz5YUW")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "yaG9Qpjm5fmSyE2L5hU8uM54pvLbY5eC0iAzWms9Xd2CB")

	val conf = new SparkConf().setAppName("TwitterPopularity")
	val ssc = new StreamingContext(conf, Seconds(2))
	val dstream = TwitterUtils.createStream(ssc, None).filter(status => status.getLang=="en")
	def hashTags = dstream.flatMap(tweet=>{
		val words = tweet.getText.split("[ :]")
		val htags = words.filter(_.startsWith("#"))
		val users = words.filter(_.startsWith("@")).map(_.replaceAll("@"," ")).toList ::: List(tweet.getUser.getName)
		htags.map(i=>(i,(users,1)))
	})

	val topCounts = hashTags.reduceByKeyAndWindow((a,b) => ((a._1 ::: b._1).distinct, a._2+b._2),Seconds(10))
				.map{case(tag,(users,count)) => (count, (tag, users))}
				.transform(_.sortByKey(false))
	topCounts.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, (tag, users)) => println("%s (%s tweets): users %s ".format(tag, count,users))}
    })
	
    ssc.start()

    ssc.awaitTermination()
}
