package com.shekhargulati.sentiment_analyzer
/**
  * Created by 12050jr on 30.01.17.
  */


import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j._
import twitter4j.auth.AuthorizationFactory._
import Twitter._
import com.opencsv.CSVWriter
import edu.stanford.nlp.util.PropertiesUtils
import twitter4j._

import scala.concurrent._
import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.util.control.Breaks
import scala.util.control.Breaks._

//corenlp language proccessing:
import java.util.Properties
import java.util.Date._
import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.io._
import scala.io.Source
import java.util.UUID

import org.joda.time.DateTime

import com.shekhargulati.sentiment_analyzer.Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._

import com.opencsv

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
//import org.apache.spark.sql.SQLContext


object Sentiment_analyzer{

  var myString= List[String]() //= new Seq[Char]

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

    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[1]")

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
        t.getText.contains("") //&& tags.contains("#food")
      }
    //val stream = TwitterUtils.createStream(streamingSparkContext, Some(auth), filters).filter(_.getLang() == "en")

      tweets.saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Trump_raw/")

    def getSentiments(input: String): List[(String, Sentiment)] = Option(input) match {
        case Some(text) if !text.isEmpty => com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiments(text)
        case _ => throw new IllegalArgumentException("input can't be null or empty")
      }

    def getSentiment(input: String): String = Option(input) match {
      case Some(text) if !text.isEmpty => com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiment(text)
      case _ => throw new IllegalArgumentException("input can't be null or empty")
    }

    //TestCases:
//      var trySen="I hate Trump, he must clearly resign being the worst president."
//      print(trySen+"\n")
//      print("Sentiment: "+getSentiment(trySen))
//
//      trySen="\nI loved this great feeling."
//      print(trySen+"\n")
//      print("Sentiment: "+getSentiment(trySen)+"\n")


    var data = tweets.map {status =>
        var sentiment = com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiment(status.getText) //getSentiment(status.getText)
        var sentimentValue=com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.convertSentimentToValue(sentiment)
      //used to be SentimentAnalysisUtils.detectSentiment()
        var tags = status.getHashtagEntities.map(_.getText().toLowerCase)
        var statusText=status.getText().replaceAll("\\,","")

      //geolocation might still have comma in it
        //var geoLocUser=status.getUser.getLocation//.replaceAll("\\,"," ")
        //var geoLocUserToString=status.getUser.getLocation.replaceAll("\\,","")

      var geoLocUserToString=Option(status.getUser).map{ _.getLocation}.getOrElse("Null")
      //var geoLocUserToString=Option(status.getUser).map{ _.getLocation}.map{l=>l.replaceAll("\\,"," ")}.getOrElse(0)
      var geoLocUserToString2=Option(geoLocUserToString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")

        //var geoLocStat=status.getGeoLocation

      //var geoLocLat=geoLocStat.getLatitude
//        var geoLocLatString=geoLocLat.toString
//        .replaceAll("\\,","\\*")
      var geoLocLatString=Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse("Null")


      //var geoLocLon=geoLocStat.getLongitude
//      var geoLocLonString=geoLocLon.toString
//        .replaceAll("\\,","\\*")
      var geoLocLonString=Option(status.getGeoLocation).map{ _.getLongitude}.getOrElse("Null")

      var statusCountryString=Option(status.getPlace).map{ _.getCountry}.getOrElse("Null")
      var statusCountryString2=Option(statusCountryString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")

      var statusPlaceString=Option(status.getPlace).map{ _.getName}.getOrElse("Null")
      var statusPlaceString2=Option(statusPlaceString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")
      var df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var dateTimeTableau=df.format(status.getCreatedAt().getTime)

      //        var geoLocLonString=""
//        var geoLocLatString=""
//
//        if(geoLocStat!=null){
//          var geoLocLat=geoLocStat.getLatitude
//          var geoLocLatString=geoLocLat.toString
//            .replaceAll("\\,","\\*")
//
//          var geoLocLon=geoLocStat.getLongitude
//          var geoLocLonString=geoLocLon.toString
//            .replaceAll("\\,","\\*")
//        }
           //geoLocLat,geoLocLon,*/
        //(status.getUser,status.getGeoLocation,status.getPlace,status.getText(), sentiment.toString(), tags.mkString(" "))
      //(geoLocUserToString, geoLocStat.getLatitude, geoLocStat.getLongitude, status.getPlace.getCountry, status.getPlace.getName, statusText, sentiment.toString, tags.mkString(" "), status.getLang,status.getCreatedAt())
      //(geoLocUserToString2," ")
      //(geoLocUserToString2, geoLocLatString, geoLocLonString, status.getPlace.getCountry, status.getPlace.getName, statusText, sentiment.toString, tags.mkString(" "), status.getLang,status.getCreatedAt())
      (geoLocUserToString2, geoLocLatString, geoLocLonString,statusCountryString2,statusPlaceString2, statusText, sentiment.toString, sentimentValue, tags.mkString(" "), status.getLang,status.getCreatedAt().getDate,status.getCreatedAt().getMonth,status.getCreatedAt().getYear ,dateTimeTableau,status.getContributors,status.getFavoriteCount,status.getRetweetCount,status.isFavorited)
    }//in tableau: Dateparse('EEE MMM dd hh:mm:ss Z yyyy', [YourString])
    // convertable in yyyy-MM-dd HH:mm:ss

    data.map(l=>l.toString().replaceAll("\\(",""))
      .map(l=>l.replaceAll("\\)",""))
      .map(l=>l.replaceAll("\\n",""))
      .map(l=>l.trim)
      .saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")

    //data.saveAsObjectFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment_objectfiles/")


    //data.csv
    //var datardd=

    //data.saveAsTextFiles("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")
    //cat /Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/part* > 2017_02_05_Output_Sentiment_Location.csv
    //data.write.format("csv").save("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/2017_02_05_Output_Sentiment_Location.csv")
    //data.write.repartition(1).format("com.databricks.spark.csv").option("header", "true").save("myfile.csv")

    //data.coalesce(1, true).saveAsTextFile(path)

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
    timerStreamMin(30,"m")
    //close all operations gracefully
    ssc.stop(false,true)


    //********** write into text and then into .csv: **************

    //function which lists subfolders
    def listMyFolders(path:String): Array[String]= {
      var file = new File(path);

      var my_filter = new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          return new File(dir, name).isDirectory()
        }
      }

      var dir: Array[String] = file.list(my_filter)
      return dir
    }

    //List all the directories in a given path
    var fileInput: String="/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/"
    var directories: Array[String]=listMyFolders(fileInput)

    //fileInput = "/Users/assansanogo/Downloads/outY/"
    //directories = listMyFolders(fileInput)
    //var mytext5 =sc.makeRDD("","")// new RDD[String]

    //create new file
    val finalTxtFile = new File("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/2017_02_05_Output_Sentiment_Location")
    //create buffered writer
    val bw = new BufferedWriter(new FileWriter(finalTxtFile))

    var mytextFinal = sc.textFile(fileInput+ "/*/*")
    var fileColl = mytextFinal.collect()
    var mytext5=sc.parallelize(fileColl)
    mytext5.saveAsTextFile(s"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/regrouped")

    for (el <- directories) {
      /*var mytext3 = sc.wholeTextFiles(fileInput + el.toString()).map(l=>l._2).
      map(l=>l.split("\\s").filter(l=>l.startsWith("#")).mkString(","))
        .flatMap(l=>l.split(","))
      var mytext3 = sc.wholeTextFiles(fileInput + el.toString()).map(l => l._2).map(l => l.split("\\H").mkString(","))
        .flatMap(l => l.split(","))
      var uniqueKey = UUID.randomUUID()
      mytext3.saveAsTextFile(fileInput + uniqueKey.toString())*/

      breakable {
        if (listMyFolders(fileInput + "/" + el.toString + "/").contains("part")) {
          //var sName = sc.textFile(fileInput + "/" + el.toString + "/part*").name
          var mytext4 = sc.textFile(fileInput + el.toString + "/part*")
          var fileColl = mytext4.collect()
          var mytext5=sc.parallelize(fileColl)
          mytext5.saveAsTextFile(s"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")//fileInput+"Solution/")

          //do following for all text files: in folder
          val part0 = Source.fromFile(fileInput + el.toString + "/part*").getLines
          //var concatFiles = concatFiles + part0.toList
          //myString= myString:::part0.toList
          myString= List.concat(myString, part0.toList)
          print(myString.toString())

        } else {
          break() //throw new IllegalArgumentException("Input can't be null or empty or matching 0 files")
        }


        //        var sName=sc.textFile(fileInput + "/" + el.toString + "/part*").name
        //        if (sc.textFile(fileInput + "/" + el.toString + "/part*").name == null) {
        //          var mytext4 = sc.textFile(fileInput + "/" + el.toString + "/part*")
        //          var myString = mytext4.collect()
        //        }else{
        //          break()
        //        }
        //
        //

         /*match {

//        case sc.textFile(fileInput + "/" + el.toString + "/part*") if !text.isEmpty => com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiments(text)
//        case _ => throw new IllegalArgumentException("input can't be null or empty")
          case (sName) if listMyFolders(fileInput + "/" + el.toString + "/").contains("part")=>
            var sName=sc.textFile(fileInput + "/" + el.toString + "/part*").name
            var mytext4 = sc.textFile(fileInput + "/" + el.toString + "/part*")
            //myString = mytext4.collect()

            //do following for all text files: in folder
            val part0 = Source.fromFile(fileInput+"/"+el.toString +"/part*").getLines
            //var concatFiles = concatFiles + part0.toList
            myString = myString + part0.toString()
          case _ => break()//throw new IllegalArgumentException("Input can't be null or empty or matching 0 files")
          }*/
      }
    }
      /*if (sc.textFile(fileInput+"/"+el.toString +"/part*").count()!=0){
        var mytext4= sc.textFile(fileInput+"/"+el.toString +"/part*")
        var myString= mytext4.collect()
      }*/

      //add
      myString.foreach(p => bw.write(p + "\n"))

      //Close the buffered writer
      bw.close

      // /Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/part-00001

    //test case of cocatenating two files:
      /*val part0 = Source.fromFile("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/part-00000").getLines
      val part1 = Source.fromFile("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/part-00001").getLines

      val part2 = part0.toList ++ part1.toList

      val part00002 = new File("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/part-00002")

      val bw1 = new BufferedWriter(new FileWriter(part00002))
      part2.foreach(p => bw1.write(p + "\n"))

      //Close the buffered writer
      bw1.close
*/
//    var mytext5=sc.parallelize(myString)
//    mytext5.saveAsTextFile(fileInput+"/MyText5/")
    
    //var file3 = new File("/Users/assansanogo/Downloads/outZ/") 

    //directories= file3.list(my_filter)
    /*//do following for all text files: in folder
    val part0 = Source.fromFile("part-00000.txt").getLines


    //add all files
    val concatFiles = part0.toList ++ part1.toList

    //create new file
    val finalTxtFile = new File("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/2017_02_05_Output_Sentiment_Location")
    //create buffered writer
    val bw = new BufferedWriter(new FileWriter(finalTxtFile))
    //add
    concatFiles.foreach(p => bw.write(p + "\n"))
    //Close the buffered writer
    bw.close*/


    //csvWriter to store all in csv
    /*val csvWriter = new CSVWriter(new FileWriter("/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/2017_02_05_Output_Sentiment_Location.csv"), '\t')
    //val entries = data.split(",");
    //val  Splitting="\v".r //:Pattern=Pattern.compile("")
    // feed in your array (or convert your data to an array)
    val entries = "first\nsecond\nthird".split("\n")
    //var entries="I dont get why this is not that easy.\nNot at all.".split(".")//data.toString.split(",")
    csvWriter.writeNext(entries)
    csvWriter.close()*/

    print(0)

  }


}

class SentimentAnalyzerSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {
//difference equal and be?
    //no value of NOT_UNDERSTOOD in Sentiment()?
/*    it("should detect NOT_UNDERSTOOD sentiment") {
      val input = ""
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should be(Sentiment.NOT_UNDERSTOOD)
    }*/

    it("should detect a VERY_NEGATIVE sentiment") {
      val input = "I hate Trump, he must DIE. I am feeling very very sad and totally bad frustrated."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_NEGATIVE)
    }
    it("should detect a VERY_NEGATIVE sentiment") {
      val input = "This is fucking shit and sucks."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_NEGATIVE)
    }

    it("should detect a VERY_NEGATIVE sentiment") {
      val input = "Worst possible thing that could've happened."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_NEGATIVE)
    }

    it("should detect a VERY_NEGATIVE sentiment") {
      val input = "I am feeling very sad and frustrated."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_NEGATIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "Dhoni laments bowling, fielding errors in series loss"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEGATIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "Man, i am losing every single game."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEGATIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "I can't believe this mistake still fails to be corrected."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEGATIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "The newspaper spoke in an unfriendly way about the poor guy."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEGATIVE)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "I am reading a book"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEUTRAL)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "A banana is yellow."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEUTRAL)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "We will leave soon."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEUTRAL)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "The review has been shared throughout newspapers."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEUTRAL)
    }

    it("should detect a POSITIVE sentiment") {
      val input = "It was a nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.POSITIVE)
    }

    it("should detect a POSITIVE sentiment") { //used to be return POSITIVE when input has positive emotion
      val input = "Scala is a great general purpose language."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.POSITIVE)
    }

    it("should detect a POSITIVE sentiment") {
      val input = "This made me happy all day."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.POSITIVE)
    }

    it("should detect a POSITIVE sentiment") { //used to be return POSITIVE when input has positive emotion
    val input = "What a wonderful idea!"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.POSITIVE)
    }

    it("should detect a VERY_POSITIVE sentiment") {
      val input = "It was a very nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_POSITIVE)
    }

    it("should detect a VERY_POSITIVE sentiment") {
      val input = "I loved this great feeling."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_POSITIVE)
    }
    it("should detect a VERY_POSITIVE sentiment") {
      val input = "This is the most amazing thing I've ever seen."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_POSITIVE)
    }
    it("should detect a VERY_POSITIVE sentiment") {
      val input = "Astonishing, how much passion and love came up with this idea."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_POSITIVE)
    }

  }
}

object Sentiment extends Enumeration {

    type Sentiment = Value

    val   VERY_NEGATIVE,NEGATIVE, NEUTRAL, POSITIVE, VERY_POSITIVE = Value //NOT_UNDERSTOOD,

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
      //case 0 => Sentiment.NOT_UNDERSTOOD
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
/*  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(PropertiesUtils.asProperties(
    "annotators", "tokenize,ssplit,pos,lemma,parse,natlog",
    "ssplit.isOneSentence", "true",
    "parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz",
    "tokenize.language", "en"))*/

  def mainSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentimentLongestSentence(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }


  def extractSentimentLongestSentence(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  def extractSentiment(text: String): String = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .maxBy { case (sentence, _) => sentence.toString }
      ._2
      .toString
  }

  def convertSentimentToValue(sent: String): Integer = {
    var result = -1

    if(sent.toString()=="VERY_NEGATIVE"){
      result=0
    }
    if(sent.toString()=="NEGATIVE"){
      result=1
    }
    if(sent.toString()=="NEUTRAL"){
      result=2
    }
    if(sent.toString()=="POSITIVE"){
      result=3
    }
    if(sent.toString()=="VERY_POSITIVE"){
      result=4
    }
//    case 0 => Sentiment.VERY_NEGATIVE
//    case 1 => Sentiment.NEGATIVE
//    case 2 => Sentiment.NEUTRAL
//    case 3 => Sentiment.POSITIVE
//    case 4 => Sentiment.VERY_POSITIVE
    result
  }
//val (_,sentiment) =extractSentiments(sentences.toString)
//    val (_, sentiment) =extractSentiments(text)
//      .maxBy {case (sentence,_)=> sentence.toLowerCase}
//    sentiment
      //.map(_._2)

    /*sentences
      .map(sentence => (sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map{ case (sentence,tree) => (Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .to*/


    def extractSentiments(text: String): List[(String, Sentiment)] = {
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      sentences
        .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
        .toList
    }



}



