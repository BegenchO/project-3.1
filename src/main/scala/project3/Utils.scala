package project3

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

}