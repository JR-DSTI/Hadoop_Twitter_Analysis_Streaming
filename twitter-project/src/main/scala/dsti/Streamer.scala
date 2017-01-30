package dsti
/**
  * Created by assansanogo on 21/01/17.
  */
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j._
import twitter4j.auth.AuthorizationFactory._
import Twitter._
//import org.apache.hive._
import java.io._
import java.util.UUID
import org.joda.time.DateTime
import java.util.Date._
import scala.concurrent._
//import org.apache.logging
import org.apache.log4j
import twitter4j.auth.{AuthorizationFactory, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder

/**
 * @author ${user.name}
 */

object Streamer {

  //clean text of unnecessary things
  def clean_string(s: String, csv: Boolean = false): String = {
    var t = s.replaceAll("[()]", "").toLowerCase();
    if (csv == true) {
      t.replaceAll(",[/s+]", ",");
    }
    return t
  }

  //frequency/no of occurrences of words
  def wordcount(s: String): Array[String] = {
    var arr = s.trim().split(" ")
    return arr
  }


  def main(args: Array[String]) {

    //initialize()

    //var b = new bclass()
    //setup all parts of the twitter sentiment

    //insert twitter credentials:
    //"consumerKey"
    val consumerKey = "dS4yzE7tzvxD9cfrLCjjf8z6c"
    //"consumerSecret"
    val consumerSecret = "LHYbQDlWYoCMVJ9oqjh46m2sU21vcbfg4cT2qQh5uiyrfEHj18"
    //"accessToken"
    val accessToken = "43985647-tWJwBvJegg6jwzTJDVSmHPtuwcryJJYgwwhwug5oM"
    //"accessTokenSecret"
    val accessTokenSecret = "YYkVU1DTSmomoQqLVwk1UropLV65fheRDscgHvXPPAQmZ"

    //transfer the parameters
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    //create auth for app
    var bc = new ConfigurationBuilder()
    bc.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret);

    val auth = new OAuthAuthorization(bc.build)

    //var twitterAuth =  new AuthorizationFactory().

    //create a spark stream of twitter here
    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[2]")
    val sc = new SparkContext(config)
    //sc.setLogLevel("WARN")

    //var mylines=sc.textFile("file:/Users/assansanogo/Downloads/positive-words.txt")
    //mylines.saveAsTextFile("file:///Users/assansanogo/Downloads/AT6")

    //open own spark streaming tab
    val streamingSparkContext = new StreamingContext(sc, Seconds(10))
    // streamingSparkContext.start()
    var filters: Array[String] = new Array[String](10)
    filters(1) = "#Trump"
    filters(2)="#trump"
    filters(3)="trump"

    //create stream with data that needs to be retrieved
    val stream = TwitterUtils.createStream(streamingSparkContext, Some(auth), filters).filter(_.getLang() == "en")

    //val s = stream.map(l => (l.getText(), l.getCreatedAt(), l.getGeoLocation(), l.isRetweeted(), l.getUser(), l.getFavoriteCount()))
    val s = stream.map(l => (l.getText()))
    s.saveAsTextFiles("file:///Users/assansanogo/Downloads/pre/") // do something
    // pause to avoid churning
    //streamingSparkContext.awaitTermination()
    streamingSparkContext.start()


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
    //close all operations gracefully


    timerStreamMin(10,"m")
    streamingSparkContext.stop(false, true)

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


    //function which cleans tweets ("RT @blabla: something"
    def cleantweets(l:String):String={
      //var line:String  = "RT @jimsciutto: WH advisors Bannon and Miller overruled the Dept of Homeland Security's judgment the travel ban should not apply to… "
      var pattern:String = "(\\w){2}\\s@(.+):\\s(.+)"

      var r=Pattern.compile(pattern)
      var m=r.matcher(l)
      var a:String=""
      if (m.find()) {
        a= (m.group(3))
      }
      return a
    }


    //List all the directories in a given path
    var fileInput: String ="/Users/assansanogo/Downloads/pre/"
    var directories: Array[String]=listMyFolders(fileInput)


    for (el <- directories) {
      var pattern4: String = "(http.*)"
      var mytext = sc.wholeTextFiles("file:///Users/assansanogo/Downloads/pre/" + el.toString()).map(l => (l._2))
       .map(l => l.replace("|", "    "))
       .map(l => l.split("    "))
       .map(l => l(0)).map(l => cleantweets(l))

       mytext = mytext.map(l => l.replaceAll(pattern4, "").trim())

      //save the file as a
        mytext
          .saveAsTextFile("file:///Users/assansanogo/Downloads/outX/" + DateTime.now().toString())

        mytext
          .map(l => l.split("\\s").mkString("_")).map(l => l.split("_").filter(l => l.contains("#")).mkString(" ")).map(l => l.replace("\\.*", ""))
          .saveAsTextFile("file:///Users/assansanogo/Downloads/outY/" + DateTime.now().toString())
   }

    fileInput = "/Users/assansanogo/Downloads/outY/"
    directories = listMyFolders(fileInput)

     for (el <- directories) {
        var mytext3 = sc.wholeTextFiles("file:///Users/assansanogo/Downloads/outY/" + el.toString()).map(l=>l._2).
          map(l=>l.split("\\s").filter(l=>l.startsWith("#")).mkString(","))
          .flatMap(l=>l.split(","))

        var uniqueKey = UUID.randomUUID()
          mytext3.saveAsTextFile("file:///Users/assansanogo/Downloads/outZ/" + uniqueKey.toString())
     }

     //var file3 = new File("/Users/assansanogo/Downloads/outZ/")
     //directories= file3.list(my_filter)
     var  mytext4= sc.textFile("/Users/assansanogo/Downloads/outZ/*/part*")
     var myString= mytext4.collect()
     var mytext5=sc.parallelize(myString)
     mytext5.saveAsTextFile("file:///Users/assansanogo/Downloads/outZZ/")
     //directories= file3.list(my_filter)

      //open own spark streaming tab context
      val streamingSparkContext = new StreamingContext(sc, Seconds(10))
      // streamingSparkContext.start()
      var filters: Array[String]= new Array[String](10)
      filters(1)= "#Trump"
      //filters(2)="#MachineLearning"
      //filters(3)="#BigData"

      //create stream with data that needs to be retrieved
//      val stream = TwitterUtils.createStream(streamingSparkContext, Some(auth),filters) // w/ filters
// w/o filters (for trendsearch)
      val stream = TwitterUtils.createStream(streamingSparkContext, None)

      //define what attributes the tweets need to have
      //val s= stream.map(l=>(l.getUser(),l.getText(),l.getCreatedAt(),l.getGeoLocation(),l.getPlace(), l.isRetweeted(),l.getFavoriteCount(),l.getHashtagEntities))
      val tags = stream.flatMap { status =>
        status.getHashtagEntities.map(_.getText)
      }


      //Local path to save the text files:
      //s.saveAsTextFiles("file:///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/");

      tags.countByValue()
        .foreachRDD { rdd =>
          val now = org.joda.time.DateTime.now()
          rdd
            .sortBy(_._2)
            .map(x => (x, now))
            .saveAsTextFile(s"file:///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/Trending/")
        }

      val tweets = stream.filter {t =>
        val tags2 = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
        tags2.contains("#bigdata") && tags2.contains("#food")
      }

      val data = tweets.map { status =>
        val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
        val tags2 = status.getHashtagEntities.map(_.getText.toLowerCase)

        (status.getText, sentiment.toString, tags)
      }
      // do something: pause to avoid churning
      //streamingSparkContext.awaitTermination()
    //blablabla
    //blabla
      streamingSparkContext.start()

      //set the current time
      var t_s:Long = System.currentTimeMillis();

      //set the end time of the stream analysis
      var t_end:Long = t_s+60000;


      //Wait until t_end has passed:
//      possibly changable via: Thread.sleep(60000)
      while(System.currentTimeMillis() < t_end) {

      }

      //close all operations gracefully
      streamingSparkContext.stop(false,true)



     /*for (el <- directories) {
       var mytext3 = sc.wholeTextFiles("file:///Users/assansanogo/Downloads/outZ/" + el.toString())

       file3.saveAsTextFile("file:///Users/assansanogo/Downloads/outZZ/"+ DateTime.now().toString())
     }*/



    }

}

       //var conf1 = new SparkConf().setAppName("pre2").setMaster("local")
       //var sc  = new SparkContext(conf1)
       //var mytext=sc.textFile("file:///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/2017_01_28_Test_Twitterproject.txt")
       //Take all lines of Textfile:
       //mytext.saveAsTextFile("file:///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/")

    // Access token
    // YYkVU1DTSmomoQqLVwk1UropLV65fheRDscgHvXPPAQmZ
    // Secret access token
    // 43985647-tWJwBvJegg6jwzTJDVSmHPtuwcryJJYgwwhwug5oM



