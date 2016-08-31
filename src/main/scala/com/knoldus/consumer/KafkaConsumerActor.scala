package com.knoldus.consumer

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.duration.DurationInt


class KafkaConsumerActor(consumer: Consumer, workers: ActorRef) extends Actor {

  import KafkaConsumerActor._

  val waitingTime = 1 seconds

  def receive: Receive = {
    case Read =>
      val records = consumer.read()
      records foreach { message => workers ! message }
      if (records.isEmpty) {
        context.system.scheduler.scheduleOnce(waitingTime, self, Read)
      } else {
        self ! Read
      }
  }


}


object KafkaConsumerActor {

  def props(consumer: Consumer, workers: ActorRef): Props = Props(classOf[KafkaConsumerActor], consumer, workers)

  case object Read

}