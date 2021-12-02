package project3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SparkConsumer {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder
            .appName("Kafka Source")
            .config("spark.master", "local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")


        // Perform Analysis I
        totalNumOfQualifiedLeads(spark)

        spark.streams.awaitAnyTermination()

    } // end main



    def totalNumOfQualifiedLeads(spark: SparkSession): Unit = {
        import spark.implicits._
        val rawDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("subscribe", Data.qualifiedLeads)
            .load()
            .select(col("value").cast("string"))

        // Split String into separate lines
        val splitLines = udf { s: String => s.split('\n') }
        val splitDF = rawDF
            .withColumn("line", explode(splitLines($"value")))
            .select("line")
        
        // Create split statements
        val splitStatements = Utils.createSplitStatements(Data.qualifiedLeads)
        
        // Convert string DF to csv DF
        val qualifiedLeadDF = splitDF.selectExpr(splitStatements:_*)

        // Print schema to console
        qualifiedLeadDF.printSchema()

        // Test print out all records
        /*
        qualifiedLeadDF.writeStream
            .outputMode("update")
            .format("console")
            .start()
        */
        // Print result to console
        val qualifiedLeadCount = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
        qualifiedLeadCount.writeStream
            .outputMode("complete")
            .format("console")
            .start()
    
    } // end method


} // end object