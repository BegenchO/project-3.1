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

        try {
            while(true) {

                Utils.delay()
                Utils.printLine()
                
                sendRecord(producer, Data.screeners)

                sendRecord(producer, Data.recruiters)

                sendRecord(producer, Data.qualifiedLeads)

            } // end while
            
        } catch {
            case e: Exception => {
                Utils.printLine()
                e.printStackTrace()
                Utils.printLine()
            }
        } finally {
            producer.close()
        } // end try catch


    } // end main


    def sendRecord(producer: KafkaProducer[String, String], topic: String): Unit = {
    
        val key = Utils.getKey()
        val value = Data.fetchData(topic)
        
        val record = new ProducerRecord[String, String](
            topic,
            key,
            value
        )
        producer.send(record)
        println("New record sent...")
    } // end sendData

} // end class

