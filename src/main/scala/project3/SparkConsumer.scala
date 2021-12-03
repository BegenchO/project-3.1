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
        //screeningsPerScreener(spark)

        // Perform ANALYSIS III
        contactAttemptsPerRecruiter(spark)

        // Perform ANALYSIS IV
        //offersByAction(spark)

        spark.streams.awaitAnyTermination()

    } // end main


    /**
      * ANALYSIS I
      * Determine and display on the console the total number of Qualified Leads
      * @param spark
      */ 
    def totalNumOfQualifiedLeads(spark: SparkSession): Unit = {
        
        val qualifiedLeadDF = getFormattedDF(spark, Data.qualifiedLeads)

        qualifiedLeadDF.printSchema()

        // Total number of qualified leads
        val totalQualifiedLeads = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
        totalQualifiedLeads.writeStream
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
        val recruitersDF = getFormattedDF(spark, Data.recruiters)
        recruitersDF.printSchema()

        val contactAttemptsDF = getFormattedDF(spark, Data.contactAttempts)
        contactAttemptsDF.printSchema()

        // Total number of contact attempts
        val totalContactAttempts = contactAttemptsDF.select(count("ql_id") as "Total Contact Attemtps")

        totalContactAttempts.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        // Number of contact attempts per recruiter

        // TO DO
            
    }

    
    /**
      * ANALYSIS III
      * Determine and display on the console the number of screenings and total number per screener 
      * @param spark
      */ 
    def screeningsPerScreener(spark: SparkSession): Unit = {
        val screenersDF = getFormattedDF(spark, Data.screeners)
        screenersDF.printSchema()

        val screeningsDF = getFormattedDF(spark, Data.screenings)
        screeningsDF.printSchema()

        // Total number of screenings
        val totalScreenings = screeningsDF.select(count("ql_id") as "Total Screenings")

        totalScreenings.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        // Number of screenings per screener

        // TO DO
    }
    
    /**
      * ANALYSIS IV
      * Determine and display on the console the number of offers and totals by offer action 
      * @param spark
      */ 
    def offersByAction(spark: SparkSession): Unit = {
        val offersDF = getFormattedDF(spark, Data.offers)
        offersDF.printSchema()

        // Total number of offers
        val totalOffer = offersDF.select(count("offer_action") as "Total Offers")

        totalOffer.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        // Number of offers by action type
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