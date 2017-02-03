package com.shekhargulati.sentiment_analyzer
/**
  * Created by 12050jr on 30.01.17.
  */


import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j._
import twitter4j.auth.AuthorizationFactory._
import Twitter._
import twitter4j._

import scala.concurrent._
import org.scalatest.FunSpec
import org.scalatest.Matchers

//corenlp language proccessing:
import java.util.Properties
import java.util.Date._
import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.io._
import java.util.UUID

import org.joda.time.DateTime

import com.shekhargulati.sentiment_analyzer.Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._

//Apache imports
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.logging
//import org.apache.hive._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.log4j
//import org.apache.logging


object Sentiment_analyzer{

  def main(args: Array[String]): Unit = {

    //insert twitter credentials of Jonas:
    //"consumerKey"
    val consumerKey = "dS4yzE7tzvxD9cfrLCjjf8z6c"
    //"consumerSecret"
    val consumerSecret = "LHYbQDlWYoCMVJ9oqjh46m2sU21vcbfg4cT2qQh5uiyrfEHj18"
    //"accessToken"
    val accessToken = "43985647-tWJwBvJegg6jwzTJDVSmHPtuwcryJJYgwwhwug5oM"
    //"accessTokenSecret"
    val accessTokenSecret = "YYkVU1DTSmomoQqLVwk1UropLV65fheRDscgHvXPPAQmZ"

    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[2]")

    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")

    var ssc = new StreamingContext(sc, Seconds(20))

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    var bc = new ConfigurationBuilder()

    bc.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    var auth = new OAuthAuthorization(bc.build)

    var filters: Array[String]= new Array[String](10)
    filters(1)= "#Trump"
    filters(2)="Trump"
    //filters(3)=""

    //var stream = TwitterUtils.createStream(ssc,Some(auth))
    val stream = TwitterUtils.createStream(ssc, Some(auth),filters).filter(_.getLang() == "en")



     var tags = stream.flatMap { status =>
        status.getHashtagEntities.map(_.getText)
      }
      tags.countByValue()
        .foreachRDD { rdd =>
          var now = org.joda.time.DateTime.now()
          rdd
            .sortBy(_._2)
            .map(x => (x, now))
            .saveAsTextFile(s"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Trending/")
        }


      var tweets = stream.filter { t =>
        //var tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
        t.getText.contains("#") //&& tags.contains("#food")
      }
    //val stream = TwitterUtils.createStream(streamingSparkContext, Some(auth), filters).filter(_.getLang() == "en")

      tweets.saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Trump_raw/")

    def getSentiments(input: String): List[(String, Sentiment)] = Option(input) match {
        case Some(text) if !text.isEmpty => com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiments(text)
        case _ => throw new IllegalArgumentException("input can't be null or empty")
      }
    def getSentiment(input: String): Sentiment = Option(input) match {
      case Some(text) if !text.isEmpty => com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiment(text)
      case _ => throw new IllegalArgumentException("input can't be null or empty")
    }
      var trySen="I like Assan and his incredible work."

      //print(getSentiment(trySen).mkString)

      //print(com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiments(trySen).mkString)

      var data = tweets.map { status =>
        var sentiment = getSentiment(status.getText)//com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiment(status.getText)
        //used to be SentimentAnalysisUtils.detectSentiment()
        var tags = status.getHashtagEntities.map(_.getText().toLowerCase)

        /*for (x <- sentiment) {
          (status.getText, x, tags)  // however you want to format it
        }*/
        (status.getGeoLocation,status.getPlace,status.getCreatedAt(),status.getLang(),status.getText(), sentiment.toString(), tags.mkString)
        /*for (sent <- sentiment)(
          sent.saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")
          )*/
      }

    data.saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")


    def timerStreamMin(a:Int,timeScale:String) {
      var t_s: Long = System.currentTimeMillis();
      var t_end: Long=0
      //if the timeScale is in hours ex: 1 hour of Streaming
      if(timeScale=="h"){
        t_end= t_s + a *3600*1000
      }
      //if the timeScale is in minutes ex: 1 min of streaming
      else if(timeScale == "m") {
        t_end = t_s + a * 60 * 1000;
      }
      //if the timeScale is in seconds ex: 1 sec of streaming
      else if (timeScale == "s"){
        t_end = t_s +a *1000
      }
      //loop that gets executed for the timeScale (preselected)
      while (System.currentTimeMillis() < t_end) {
      }
    }


    ssc.start()
    timerStreamMin(10,"s")
    //close all operations gracefully
    ssc.stop(false,true)

    print(0)

  }


}

class SentimentAnalyzerSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {
//difference equal and be?
    //no value of NOT_UNDERSTOOD in Sentiment()?

    it("should return POSITIVE when input has positive emotion") {
      val input = "Scala is a great general purpose language."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.POSITIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "Dhoni laments bowling, fielding errors in series loss"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.NEGATIVE)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "I am reading a book"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.NEUTRAL)
    }

    it("should detect not understood sentiment") {
      val input = ""
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.NOT_UNDERSTOOD)
    }

    it("should detect a negative sentiment") {
      val input = "I am feeling very sad and frustrated."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.VERY_NEGATIVE)
    }

    it("should detect a positive sentiment") {
      val input = "It was a nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.POSITIVE)
    }

    it("should detect a very positive sentiment") {
      val input = "It was a very nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.VERY_POSITIVE)
    }


  }
}

object Sentiment extends Enumeration {

    type Sentiment = Value

    val POSITIVE, NEGATIVE, NEUTRAL, NOT_UNDERSTOOD, VERY_NEGATIVE, VERY_POSITIVE = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
      case -1 => Sentiment.NOT_UNDERSTOOD
      case 0 => Sentiment.VERY_NEGATIVE
      case 1 => Sentiment.NEGATIVE
      case 2 => Sentiment.NEUTRAL
      case 3 => Sentiment.POSITIVE
      case 4 => Sentiment.VERY_POSITIVE
    }
  }

object SentimentAnalyzer {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def mainSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }


  private def extractSentimentLongestSentence(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  /*def extractSentiment(text: String): Sentiment = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences.toString(sentence => (sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
       { case (tree) => (Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }

  }*/

  def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }


}



