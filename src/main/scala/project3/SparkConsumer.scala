package project3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object SparkConsumer {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder
            .appName("Kafka Source")
            .config("spark.master", "local[*]")
            .config("spark.streaming.concurrentJobs","2")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")


        // Perform ANALYSIS I
        //totalNumOfQualifiedLeads(spark)

        // Perform ANALYSIS II


        // Perform ANALYSIS III


        // Perform ANALYSIS IV
        offersByAction(spark)

        spark.streams.awaitAnyTermination()

    } // end main


    /**
      * ANALYSIS I
      * Determine and display on the console the total number of Qualified Leads
      * @param spark
      */ 
    def totalNumOfQualifiedLeads(spark: SparkSession): Unit = {
        
        val qualifiedLeadDF = getFormattedDF(spark, Data.qualifiedLeads)

        // Print schema to console
        qualifiedLeadDF.printSchema()

        // Print result to console
        val qualifiedLeadCount = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
        qualifiedLeadCount.writeStream
            .outputMode("complete")
            .format("console")
            .start()
    
    } // end method


    /**
      * ANALYSIS II
      * Determine and display on the console the number of contact attempts and total number per recruiter
      * @param spark
      */ 
    def contactAttemptsPerRecruiter(spark: SparkSession): Unit = {

    }

    
    /**
      * ANALYSIS III
      * Determine and display on the console the number of screenings and total number per screener 
      * @param spark
      */ 
    def screeningsPerScreener(spark: SparkSession): Unit = {
        
    }
    
    /**
      * ANALYSIS IV
      * Determine and display on the console the number of offers and totals by offer action 
      * @param spark
      */ 
    def offersByAction(spark: SparkSession): Unit = {
        val offersDF = getFormattedDF(spark, Data.offers)
        offersDF.printSchema()

        // Total offer count
        val totalOffer = offersDF.select(count("offer_action") as "total")

        totalOffer.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        // Total by offer action
        val offerByAction = offersDF.select("offer_action").groupBy("offer_action").count()

        offerByAction.writeStream
            .outputMode("complete")
            .format("console")
            .start()

    } // end offersByAction


    /**
      * Returns DF with schema by subscribing to a Kafka topic
      *
      * @param spark
      * @param topic
      */
    def getFormattedDF(spark: SparkSession, topic: String): DataFrame = {
        import spark.implicits._
        val rawDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("subscribe", topic)
            .load()
            .select(col("value").cast("string"))

        // Split String into separate lines
        val splitLines = udf { s: String => s.split('\n') }
        val splitDF = rawDF
            .withColumn("line", explode(splitLines($"value")))
            .select("line")
        
        // Create split statements
        val splitStatements = Utils.createSplitStatements(topic)
        
        // Convert string DF to csv DF by splitting into columns
        val formattedDF = splitDF.selectExpr(splitStatements:_*)

        formattedDF
    }


} // end object