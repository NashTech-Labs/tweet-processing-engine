package com.knoldus.persistence

import akka.actor.{Actor, Props}
import org.slf4j.LoggerFactory


class PersistenceActor(esService: ESService) extends Actor {

  import PersistenceActor._

  val logger = LoggerFactory.getLogger(this.getClass())

  def receive: Receive = {
    case Documents(feed) =>
      esService.persist(feed)

    case invalidMessage =>
      logger.warn("No handler for this message " + invalidMessage)

  }

}

object PersistenceActor {

  def props(esService: ESService) = Props(classOf[PersistenceActor], esService)

  case class Documents(feed: List[Map[String, String]])

}

trait ESService {

  def persist(feed: List[Map[String, String]]): Long

}

