package project3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SparkConsumer {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("Kafka Source")
            .config("spark.master", "local[*]")
            .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val initDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("subscribe", Data.screeners)
            .load()
            .select(col("value").cast("string"))

        initDF.writeStream
            .outputMode("update")
            .format("console")
            .start()
            .awaitTermination()

    } // end main

} // end object