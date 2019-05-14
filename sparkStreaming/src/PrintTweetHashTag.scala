package com.practice.sparkstreaming

import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

   object PrintTweetHashTag {

    def main(args: Array[String]): Unit = {
    setupTwitter()

    val ssc = new StreamingContext("local[*]","PrintTweets",Seconds(1))

    setupLogging()


    val tweets = TwitterUtils.createStream(ssc,None)

    val status1 = tweets.flatMap(status=>status.getText.split(" ").filter(words=>words.startsWith("#")))
    //val status2 = status1.map(t=>(t,1)).reduceByKey((x,y)=>x+y)
      val status2 = status1.map(t=>(t,1)).reduceByKeyAndWindow((x,y)=>x+y,Seconds(1000))
          .map{case(topic,count)=>(count,topic)}.transform(_.sortByKey(false))

      ssc.checkpoint("E:\\checkpoint")

    status2.print()
    ssc.start()
    ssc.awaitTermination()




  }






}
