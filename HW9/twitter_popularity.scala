package org.apache.spark.examples.streaming.twitter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{LogManager, Level}
import org.apache.commons.logging.LogFactory

object twitter_popularity {
	def main(args: Array[String]) {
	
		LogManager.getRootLogger().setLevel(Level.ERROR)

		if (args.length != 3) {
			System.err.println("\n\n\tUsage: twitter_popularity <duration in minutes> <sampling frequency seconds> <number of samples to take>")
			System.exit(1)
		}
		val Array(firstarg,secondarg,thirdarg) = args.take(3)
		val duration = firstarg.toInt * 60
		val samplingFrequency = secondarg.toInt
		val samplingNumber = thirdarg.toInt
		
	    System.setProperty("twitter4j.oauth.consumerKey", "lMHMMbmNxtlLHcZA9Bqxh8h6w")
	    System.setProperty("twitter4j.oauth.consumerSecret", "KAT8bFoZknI1TYnx19Gx6p4cQqMbTtFAUOoBf1ndmHQ7eskgEX")
	    System.setProperty("twitter4j.oauth.accessToken", "934581358344790016-yq3GrSkXlZoTM0Yskv9b2DoTNBz5YUW")
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "yaG9Qpjm5fmSyE2L5hU8uM54pvLbY5eC0iAzWms9Xd2CB")
	
		val conf = new SparkConf().setAppName("TwitterPopularity")
		val ssc = new StreamingContext(conf, Seconds(samplingFrequency))
		LogManager.getRootLogger().setLevel(Level.ERROR)

		val dstream = TwitterUtils.createStream(ssc, None).filter(status => status.getLang=="en")
		var totalDuration = 0
		def hashTags = dstream.flatMap(tweet=>{
			val words = tweet.getText.split("[ :]")
			val htags = words.filter(_.startsWith("#"))
			val users = words.filter(_.startsWith("@")).map(_.replaceAll("@"," ")).toList // ::: List(tweet.getUser.getId)
			htags.map(i=>(i, (tweet.getUser.getName :: Nil, users,1)))
		})

		var cumulativeRDD : org.apache.spark.rdd.RDD[(Int, (String, List[String], List[String]))] = ssc.sparkContext.emptyRDD
		var dStreamRDDList = ssc.sparkContext.emptyRDD
//		val topCounts = hashTags.reduceByKeyAndWindow(
//				windowDuration=Seconds(duration),
//				reduceFunc = {(a,b) => ((a._1 ::: b._1).distinct, a._2+b._2)}
//			)
		val topCounts = hashTags.reduceByKey( (a,b) => ((a._1 ::: b._1).distinct, (a._2 ::: b._2).distinct, a._3+b._3))
			.map{case(tag,(authors, users,count)) => (count, (tag, authors, users))}
			.transform(_.sortByKey(false))

		topCounts.foreachRDD(rdd => {
			val topList = rdd.take(samplingNumber)
			totalDuration = totalDuration + samplingFrequency
			if (totalDuration >= duration) {
				print("\n\nEnding: cumulative summary\n")
				cumulativeRDD = cumulativeRDD union rdd
				val lastList = cumulativeRDD.sortByKey(false).take(samplingNumber)
				println("\nThe top %d most popular topics in last %d seconds (out of %s total, start time %s seconds ago):".format(samplingNumber, duration, cumulativeRDD.count(), totalDuration))
				lastList.foreach{case (count, (tag, author, users)) => println("%s tweets with the tag %s \n\t author %s: \n\t referenced user %s ".format(count, tag, author, users))}
				ssc.stop()
			}
			else {
				println("\nThe top %d most popular topics in last %d seconds (out of %s total, start time %s seconds ago):".format(samplingNumber, samplingFrequency, rdd.count(), totalDuration))
				topList.foreach{case (count, (tag, author, users)) => println("%s tweets with the tag %s \n\t author %s: \n\t referenced user %s ".format(count, tag, author, users))}
				cumulativeRDD = cumulativeRDD union rdd
			}
	    })
		
	    ssc.start()
	
	    ssc.awaitTermination()
	}
}
