package com.knoldus.consumer

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


class Consumer(groupId: String, servers: String, topics: List[String]) {

  private val timeout = 10000

  val logger = LoggerFactory.getLogger(this.getClass())

  private val props: Properties = new Properties
  props.put("bootstrap.servers", servers)
  props.put("client.id", UUID.randomUUID.toString)
  props.put("group.id", groupId)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)

  private val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(topics)

  def read(): List[MessageFromKafka] = {
    try {
      logger.info("Reading from kafka queue ...... " + topics)
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(timeout)
      consumerRecords.map(record => MessageFromKafka(record.value())).toList
    }
    catch {
      case wakeupException: WakeupException => {
        logger.error(" Getting WakeupException ", wakeupException)
        Nil
      }
    }
  }

  def close(): Unit = consumer.close()

}

case class MessageFromKafka(record: String)
