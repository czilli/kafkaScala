package kafkaInScala.scalaProducer

import java.util.{ Properties, Scanner }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }

object Producer extends App {
  val props = new Properties();
  props.put("bootstrap.servers", "localhost:9092");
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("request.required.acks", "1");

  val producer = new KafkaProducer[String, String](props)
  val scanner = new Scanner(System.in);

  var run = true;
  try {
    while (run) {
      val input = scanner.nextLine();

      if ("q" == input) {
        run = false;
      }

      val data = new ProducerRecord[String, String]("test", input);
      producer.send(data);
    }

  } finally {
    scanner.close();
    producer.close();
  }

}