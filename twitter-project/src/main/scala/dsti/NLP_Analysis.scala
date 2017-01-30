package dsti

/**
  * Created by 12050jr on 30.01.17.
  */

//corenlp language proccessing:
import edu.stanford.nlp.simple._
import edu.stanford.nlp._
import sentiment.SentimentUtils._
import org.scalatest.{FunSpec, Matchers}
import java.util.Properties

//poses problem here:
import com.shekhargulati.sentiment_analyzer.Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._


class NLP_Analysis {


  object Sentiment extends Enumeration {
    type Sentiment = Value
    val POSITIVE, NEGATIVE, NEUTRAL = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
      case x if x == 0 || x == 1 => Sentiment.NEGATIVE
      case 2 => Sentiment.NEUTRAL
      case x if x == 3 || x == 4 => Sentiment.POSITIVE
    }
  }

  //sentiment analysis over NLP edu.stanford.nlp:
  def detectSentiment(message: String): nlp.sentiment.type = {
    //SENTIMENT_TYPE
    describe("sentiment analyzer") {

      it("should detect not understood sentiment") {
        detectSentiment("") should equal(NOT_UNDERSTOOD)
      }

      it("should detect a negative sentiment") {
        detectSentiment("I am feeling very sad and frustrated.") should equal(NEGATIVE)
      }

      it("should detect a neutral sentiment") {
        detectSentiment("I'm watching a movie") should equal(NEUTRAL)
      }

      it("should detect a positive sentiment") {
        detectSentiment("It was a nice experience.") should equal(POSITIVE)
      }

      it("should detect a very positive sentiment") {
        detectSentiment("It was a very nice experience.") should equal(VERY_POSITIVE)
      }
    }
  }


  class SentimentAnalyzerSpec extends FunSpec with Matchers {

    describe("sentiment analyzer") {

      it("should return POSITIVE when input has positive emotion") {
        val input = "Scala is a great general purpose language."
        val sentiment = SentimentAnalyzer.mainSentiment(input)
        sentiment should be(Sentiment.POSITIVE)
      }
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

    private def extractSentiment(text: String): Sentiment = {
      val (_, sentiment) = extractSentiments(text)
        .maxBy { case (sentence, _) => sentence.length }
      sentiment
    }

    def extractSentiments(text: String): List[(String, Sentiment)] = {
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      sentences
        .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
        .toList
    }

  }

}