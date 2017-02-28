package com.shekhargulati.sentiment_analyzer
/**
  * Created by 12050jr on 30.01.17.
  */


//Twitter
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j._
import twitter4j.auth.AuthorizationFactory._
import Twitter._

//Scala imports
import scala.concurrent._
import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.util.control.Breaks
import scala.util.control.Breaks._
import scala.collection.convert.wrapAll._
import scala.io.Source

//Java imports
import java.util.Properties
import java.util.Date._
import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.io._
import java.util.UUID

//corenlp language proccessing:

import com.shekhargulati.sentiment_analyzer.Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.PropertiesUtils

//Data processing
import org.joda.time.DateTime
import com.opencsv
import com.opencsv.CSVWriter

//Apache imports
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.log4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs._


//import org.apache.logging
//import org.apache.hive._
//import org.apache.logging
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}

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

    //Set up a new Sparkconfig for the yarn master
    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("yarn-client") //yarn-cluster //) //"local[2]"

    //Set up a new Sparkcontext
    val sc = new SparkContext(config)

    //Set the Logging level of the transactions
    sc.setLogLevel("WARN")

    //Set the streaming context
    var ssc = new StreamingContext(sc, Seconds(20))

    //Put the Twitter credentials
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

    //Set array for filtering the twitter stream
    var filters: Array[String]= new Array[String](10)
    filters(1)= "#Trump"
    filters(2)="Trump"
    //filters(3)=""


    //create the stream of tweets and filter for english language
    val stream = TwitterUtils.createStream(ssc, Some(auth),filters).filter(_.getLang() == "en")
    //var stream = TwitterUtils.createStream(ssc,Some(auth))

    //map tweets
     var tags = stream.flatMap { status =>
        status.getHashtagEntities.map(_.getText)
     }

    //Sort trending trending tweets by hashtags
    tags.countByValue()
      .foreachRDD { rdd =>
        var now = org.joda.time.DateTime.now()
        rdd
          .sortBy(_._2)
          .map(x => (x, now))
          .map(l=>l.toString().replaceAll("\\(",""))
          .map(l=>l.replaceAll("\\)",""))
          .map(l=>l.replaceAll("\\n",""))
          .map(l=>l.trim)
          .saveAsTextFile(s"/user/jonas/twitterproject/Tweets_Trending/") //s"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Trending/")
      }

    //set raw tweets filtering: possibility to filter tweets by certain keywords
    var tweets = stream.filter { t =>
      //var tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      t.getText.contains("")
    }

    //save the raw tweets:
    tweets.saveAsTextFiles("/user/jonas/twitterproject/Tweets_Trump_raw/")//"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Trump_raw/")

    //Define functions to get the sentiments of the tweets
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

    //map all tweets into appropriate form for analysis and preprocess them with removing spaces and commatas
    var data = tweets.map {status =>
      //Set the sentiment of the tweet's text (to improve the accuracy, a preprocessing of the text itself could be done)
      var sentiment = com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.extractSentiment(status.getText)

      //Set the sentiment's integer value (0-very negative to 4-very positive)
      var sentimentValue=com.shekhargulati.sentiment_analyzer.SentimentAnalyzer.convertSentimentToValue(sentiment)

      //collect all the Hashtags of the tweets
      var tags = status.getHashtagEntities.map(_.getText().toLowerCase)

      //Clean the text from commatas
      var statusText=status.getText().replaceAll("\\,","")

      //check if parameters a available, if y then set them, if n then set "Null"
      //Tweets location
      var geoLocUserToString=Option(status.getUser).map{ _.getLocation}.getOrElse("Null")
      var geoLocUserToString2=Option(geoLocUserToString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")

      //GeoLoc Latitude
      var geoLocLatString=Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse("Null")

      //GeoLoc Longitued
      var geoLocLonString=Option(status.getGeoLocation).map{ _.getLongitude}.getOrElse("Null")

      //Country
      var statusCountryString=Option(status.getPlace).map{ _.getCountry}.getOrElse("Null")
      var statusCountryString2=Option(statusCountryString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")

      //Place
      var statusPlaceString=Option(status.getPlace).map{ _.getName}.getOrElse("Null")
      var statusPlaceString2=Option(statusPlaceString).map{l=>l.toString.replaceAll("\\,"," ")}.getOrElse("Null")

      //Set the time format of the tweet for further processing (here suiting tableau)
      var df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var dateTimeTableau=df.format(status.getCreatedAt().getTime)

      //Return
      (geoLocUserToString2, geoLocLatString, geoLocLonString,statusCountryString2,statusPlaceString2, statusText, sentiment.toString, sentimentValue, tags.mkString(" "), status.getLang,status.getCreatedAt().getDate,status.getCreatedAt().getMonth,status.getCreatedAt().getYear ,dateTimeTableau,status.getContributors,status.getFavoriteCount,status.getRetweetCount,status.isFavorited)

    }

    //preprocess resulting data, removing brackets, new lines and spaces in the end and beginning
    data.map(l=>l.toString().replaceAll("\\(",""))
      .map(l=>l.replaceAll("\\)",""))
      .map(l=>l.replaceAll("\\n",""))
      .map(l=>l.trim)
      .saveAsTextFiles("/user/jonas/twitterproject/Tweets_Sentiment/")//"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/")


    //function to set the Streaming duration:
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

    //Start the streaming process:
    ssc.start()

    //Set the Streaming duration
    timerStreamMin(10,"s")

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
    var fileInput: String="/user/jonas/twitterproject/Tweets_Sentiment/" //"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/"
    var directories: Array[String]=listMyFolders(fileInput)

    //fileInput = "/Users/assansanogo/Downloads/outY/"
    //directories = listMyFolders(fileInput)
    //var mytext5 =sc.makeRDD("","")// new RDD[String]

//    //create new file
    //OldVersion// val finalTxtFile = new File("/user/jonas/twitterproject/Tweets_Sentiment/OutputText")
//    //create buffered writer
    //OldVersion// val bw = new BufferedWriter(new FileWriter(finalTxtFile))

    //Collect all files in the output folder to process them
    var mytextFinal = sc.textFile("/user/jonas/twitterproject/Tweets_Sentiment/*/*")
    var fileColl = mytextFinal.collect()
    var mytext5=sc.parallelize(fileColl)
    mytext5.repartition(1).saveAsTextFile("/user/jonas/twitterproject/Tweets_Sentiment/Output/")///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/regrouped")

    //function to add header to the data set
    def addHeader(rdd2:RDD[String],header2:RDD[String]):Unit= {
      header2.union(rdd2).repartition(1).saveAsTextFile("/user/jonas/twitterproject/Tweets_Sentiment/OutputHeader/")//"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/AssanMLFiles")
    }

    //set and collect all the text files resulting from mytext5
    val rdd2 = sc.textFile("/user/jonas/twitterproject/Tweets_Sentiment/Output/*")//"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/regrouped/part-00000")
    val header2 = sc.parallelize(Array("User_geoLoc, Tweet_geoLocLat, Tweet_geoLocLon, Tweet_Country, Tweet_Place, Tweet_Text, Tweet_sentiment, Tweet_sentimentValue, Tweet_tags, Tweet_Language, Tweet_CreatedAtDate, Tweet_CreatedAtMonth, Tweet_CreatedAtYear, Tweet_CreatedAtTime,Tweet_Contributors,Tweet_FavoriteCount,Tweet_RetweetCount,Tweet_isFavorited"))

    //add header to the data set
    addHeader(rdd2,header2)

    //Set the config to create new file/folders in the hdfs
    val hdfs =FileSystem.get(new Configuration())
    val workingDir=hdfs.getWorkingDirectory()
    val newFolderPath= new Path("/user/jonas/twitterproject/Tweets_Sentiment/")

    val totalFolderPath = Path.mergePaths(workingDir, newFolderPath)

    //Delete existing Directory if available
    if(hdfs.exists(totalFolderPath))
    {
      hdfs.delete(totalFolderPath, true);
    }

    val finalTxtFile = hdfs.create(new Path("/user/jonas/twitterproject/Tweets_Sentiment/Output/OutputFile_"+ UUID.randomUUID().toString + ".csv"))//"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/AssanMLFiles/part-00000.csv") // OLD: "/user/jonas/twitterproject/Tweets_Sentiment/OutputText.txt"))

    //possible: fs.create(new Path("/tmp/mySample.txt"))
    //finalTxtFile.write("This is a test".getBytes)

//    saveAsCsv("hdfs://ec2-52-214-216-145.eu-west-1.compute.amazonaws.com:8020/user/jonas/twitterproject/Tweets_Sentiment/OutputHeader/","hdfs://ec2-52-214-216-145.eu-west-1.compute.amazonaws.com:8020/user/jonas/twitterproject/Tweets_Sentiment/OutputHeaderCSV/")
//
//    def saveAsCsv(pathSource:String="/Users/assansanogo/Downloads/MLReady/part-00000",pathDestination:String="/Users/assansanogo/Downloads/MLReady/part-00000.csv")= {
//      //save file  as a csv file
//      var fileFinal = Source.fromFile(pathSource).getLines.toList
//      val partFinal = new File(pathDestination)
//
//      var bwriter = new BufferedWriter(new FileWriter(partFinal))
//      fileFinal.foreach(p => bwriter.write(p + "\n"))
//
//      bwriter.close()
//    }
//
//    //save file as a csv file
//    var fileFinal = Source.fromFile("/user/jonas/twitterproject/Tweets_Sentiment/OutputHeader/") //"/Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Tweets_Sentiment/AssanMLFiles/part-00000").getLines.toList
//
//    //new print writer to list all the tweet outputs in appropriate form
//    var bwriter = new BufferedWriter(new FileWriter(finalTxtFile))
//    fileFinal.foreach(p => bwriter.write(p + "\n"))
//
//    //Close the writer
//    bwriter.close()



    //lift data into Hive:
    // Option 1 ========================
//    // Define which topics to read from
//    val topic = "topic_twitter"
//    val groupId = "group-1"
//    val consumer = KafkaConsumer(topic, groupId, "localhost:2181")
//
//    //Create SparkContext
//    val sparkContext = new SparkContext("local[2]", "KafkaConsumer")
//
//    //Create HiveContext
//    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
//
//    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS twitter_data (tweetId BIGINT, tweetText STRING, userName STRING, tweetTimeStamp STRING,   userLang STRING)")
//    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS demo (foo STRING)")
//    val hiveSql = "INSERT INTO TABLE twitter_data SELECT STACK( 1," +
//      tweetID        +","  +
//      tweetText      +"," +
//      userName       +"," +
//      tweetTimeStamp +","  +
//      userLang + ") FROM demo limit 1"
//
//    hiveContext.sql(hiveSql)

    // Option 2 ========================
//    create external table test(key string, value string)
//    ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
//    LOCATION '/SOME/HDFS/LOCATION'
    print(0)

  }


}

class SentimentAnalyzerSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {

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

    val   VERY_NEGATIVE,NEGATIVE, NEUTRAL, POSITIVE, VERY_POSITIVE = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {

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
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])))
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
    result
  }
    def extractSentiments(text: String): List[(String, Sentiment)] = {
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      sentences
        .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
        .toList
    }



}



