package dsti
/**
  * Created by assansanogo on 21/01/17.
  */
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
//import java.Time
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
    def clean_string (s: String, csv:Boolean =false) : String ={
      var t= s.replaceAll("[()]","").toLowerCase();
      if (csv==true){
        t.replaceAll(",[/s+]",",");
      }
      return t
    }
    //frequency/no of occurrences of words
    def wordcount (s:String): Array[String]={
      var arr = s.trim().split(" ")
      return arr
    }



    def main(args: Array[String]){

      //initialize()

      //var b = new bclass()
      //setup all parts of the twitter sentiment

      //insert twitter credentials:
      //"consumerKey"
      val consumerKey = "dS4yzE7tzvxD9cfrLCjjf8z6c"
      //"consumerSecret"
      val consumerSecret ="LHYbQDlWYoCMVJ9oqjh46m2sU21vcbfg4cT2qQh5uiyrfEHj18"
      //"accessToken"
      val accessToken ="43985647-tWJwBvJegg6jwzTJDVSmHPtuwcryJJYgwwhwug5oM"
      //"accessTokenSecret"
      val accessTokenSecret ="YYkVU1DTSmomoQqLVwk1UropLV65fheRDscgHvXPPAQmZ"

//transfer the parameters
      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


//create auth for app
      var bc= new ConfigurationBuilder()
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
      var filters: Array[String]= new Array[String](10)
      filters(1)= "#Trump"
      //filters(2)="#MachineLearning"
      //filters(3)="#BigData"

      //create stream with data that needs to be retrieved
      val stream = TwitterUtils.createStream(streamingSparkContext, Some(auth),filters)

      val s= stream.map(l=>(l.getText(),l.getCreatedAt(),l.getGeoLocation(),l.isRetweeted(),l.getUser(),l.getFavoriteCount()))
      s.saveAsTextFiles("file:///Users/12050jr/Dropbox/40_DSTI_Data Science Big Data/10_Classes/007_Hadoop Ecosystem/Project_Twitter/Output/");
      // do something
      // pause to avoid churning
      //streamingSparkContext.awaitTermination()
      streamingSparkContext.start()


      var t_s:Long = System.currentTimeMillis();
      var t_end:Long = t_s+60000;
      while(System.currentTimeMillis() < t_end) {

      }
      //close all operations gracefully
      streamingSparkContext.stop(false,true)


      //var conf1 = new SparkConf().setAppName("pre2").setMaster("local")
      //var sc  = new SparkContext(conf1)
      //var mytext=sc.textFile("file:///Users/assansanogo/Downloads/AT.txt")
      //mytext.collect
      // mytext.map(l=> clean_string(l,true)).saveAsTextFile("file:///Users/assansanogo/Downloads/AT4")
      // var mytext2=sc.textFile("file:///Users/assansanogo/Downloads/AT4/part-00000")
      // mytext2.flatMap(l=>l.split(" ")).map(l=>(l,1)).reduceByKey(_ + _).saveAsTextFile("file:///Users/assansanogo/Downloads/AT5")
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


}
