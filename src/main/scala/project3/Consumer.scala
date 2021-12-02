package project3

import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object Consumer {
    def main(args: Array[String]): Unit = {

        val props: Properties = new Properties()
        props.put("group.id", "test")
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
        
        val consumer = new KafkaConsumer(props)

        try {
            consumer.subscribe(Data.topics.asJava)
            while(true) {
                val records = consumer.poll(10)
                for (record <- records.asScala) {
                    Utils.printLine()
                    println(record.value())
                }
            }
            Utils.printLine()
        } catch {
             case e: Exception => {
                Utils.printLine()
                e.printStackTrace()
                Utils.printLine()
            }
        }


    } // end main

    
} // end object