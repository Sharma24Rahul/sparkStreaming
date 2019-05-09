package com.practice.sparkstreaming

import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import Utilities._

object PrintTweet {

  def main(args: Array[String]): Unit = {
    setupTwitter()

    val ssc = new StreamingContext("local[*]","PrintTweets",Seconds(1))

    setupLogging()

    val tweets = TwitterUtils.createStream(ssc,None)

    val status1 = tweets.map(status=>status.getText)
    status1.print()
    ssc.start()
    ssc.awaitTermination()




  }






}
