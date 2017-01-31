package dsti
package com.shekhargulati.sentiment_analyzer
/**
  * Created by 12050jr on 30.01.17.
  */

import java.util.Properties

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
      sentiment should equal(Sentiment.NEGATIVE)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "I am reading a book"
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NEUTRAL)
    }

    it("should detect not understood sentiment") {
      val input = ""
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.NOT_UNDERSTOOD)
    }

    it("should detect a negative sentiment") {
      val input = "I am feeling very sad and frustrated."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_NEGATIVE)
    }

    it("should detect a positive sentiment") {
      val input = "It was a nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.POSITIVE)
    }

    it("should detect a very positive sentiment") {
      val input = "It was a very nice experience."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      sentiment should equal(Sentiment.VERY_POSITIVE)
    }


  }
}

  object Sentiment extends Enumeration {

    type Sentiment = Value

    val POSITIVE, NEGATIVE, NEUTRAL, NOT_UNDERSTOOD, VERY_NEGATIVE, VERY_POSITIVE = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
      case 0 => Sentiment.VERY_NEGATIVE
      case 1 => Sentiment.NEGATIVE
      case 2 => Sentiment.NEUTRAL
      case 3 => Sentiment.POSITIVE
      case 3 => Sentiment.VERY_POSITIVE
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


class NLP_Analysis {











}


