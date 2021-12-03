package project3

import scala.annotation.meta.field

// CopyJarToVM              ->      scp -P 2222 project-3_2.11-0.1.0.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev/project3
// SparkConsumer.scala      ->      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --class project3.SparkConsumer project-3_2.11-0.1.0.jar
// Consumer.scala           ->      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --class project3.Consumer project-3_2.11-0.1.0.jar
// Producer.scala           ->      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --class project3.Producer project-3_2.11-0.1.0.jar

object Utils {
    def printLine(): Unit = {
        println("------------------------------------")
    }

    def addTestLine(text: String*): Unit = {
        printLine()
        println(text)
        printLine()
    }

    def delay(seconds: Int = 2): Unit = {
        Thread.sleep(seconds * 1000)
    }

    def getKey(): String = {
        val r = scala.util.Random
        val key = r.nextInt(1000).toString()
        key
    }

    def createSplitStatements(topic: String): Array[String] = {
        var fields = List[String]()
        var stringStatements = ""

        // Get list of field names based on topic
        topic match {
            case Data.screeners => fields = Data.screenersFields
            case Data.recruiters => fields = Data.recruitersFields
            case Data.qualifiedLeads => fields = Data.qualifiedLeadsFields
            case Data.contactAttempts => fields = Data.contactAttemptsFields
            case Data.screenings => fields = Data.screeningsFields
            case Data.offers => fields = Data.offersFields
        }

        // Create statements
        for (i <- 0 until fields.length) {
            stringStatements += s"split(line,',')[${i}] as ${fields(i)}~"
        }

        // Split and convert into a list of strings
        val statements = stringStatements.split("~")

        statements
    }

} // end object