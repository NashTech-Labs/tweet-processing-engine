package com.knoldus.worker

import akka.actor.{Actor, ActorRef, Props}
import com.knoldus.core.SentimentAnalyzer
import com.knoldus.persistence.PersistenceActor
import org.slf4j.LoggerFactory


class TweetProcessor(persistenceActor: ActorRef, sentimentAnalyzer: SentimentAnalyzer) extends Actor {

  import PersistenceActor._
  import TweetProcessor._

  val logger = LoggerFactory.getLogger(this.getClass())

  override def receive: Receive = {

    case Tweet(feed) =>
      val sentiment = sentimentAnalyzer.getSentiment(feed("cleaned_text"))
      val tweetWithSentiment = feed + ("sentiment" -> sentiment)
      persistenceActor ! Documents(List(tweetWithSentiment))

    case invalidMessage =>
      logger.warn("No handler for this message " + invalidMessage)


  }

}


object TweetProcessor {

  def props(persistenceActor: ActorRef) = Props(classOf[TweetProcessor], persistenceActor)

  case class Tweet(feed: Map[String, String])

}