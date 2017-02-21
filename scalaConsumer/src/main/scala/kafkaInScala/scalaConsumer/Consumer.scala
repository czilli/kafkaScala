package kafkaInScala.scalaConsumer

import java.util.{ Properties, Collections }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }

object Consumer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test_group")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList("test"))
  while (true) {
    val records = consumer.poll(100)
    val it=records.iterator()
    while(it.hasNext()){
      val record=it.next()
      System.out.println(record.value)
    }
  }
}