package project3

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
    def main(args: Array[String]): Unit = {

        val props: Properties = new Properties()
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("acks", "all")

        val producer = new KafkaProducer[String, String](props)
        val topic = "screeners"

        try {
            val record = new ProducerRecord[String, String](
                topic,
                "1",
                "1,Alisa,Figgures"
            )
            producer.send(record)
            println("Record sent...")
        } catch {
            case e: Exception => {
                Utils.printLine()
                e.printStackTrace()
                Utils.printLine()
            }
        } finally {
            producer.close()
        }


    } // end main
} // end class

