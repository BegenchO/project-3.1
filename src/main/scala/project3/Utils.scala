package project3

// --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4

object Utils {
    def printLine(): Unit = {
        println("------------------------------------")
    }

    def delay(seconds: Int = 2): Unit = {
        Thread.sleep(seconds * 1000)
    }

    def getKey(): String = {
        val r = scala.util.Random
        val key = r.nextInt(1000).toString()
        key
    }

} // end object