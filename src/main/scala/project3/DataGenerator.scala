package project3

import scala.collection.mutable.ListBuffer

/**
  * Data generator class
  * Code from Theodore's P3 implementation
  * Generates random data for each of 6 tables
  * Methods are at the very bottom
  */
object DataGenerator {

  val r = new scala.util.Random

  // Store assigned ids to use with screenings, contact atttempts and offers
  var createdScreenerIds = new ListBuffer[String]()
  var createdRecruiterIds = new ListBuffer[String]()
  var createdQualifiedLeadIds = new ListBuffer[String]()

  def valueDeterminer(topic: String): String = {

    // Creates unique ids
    val screenerid = {
      var exists = true
      var newId = ""
      while(exists) {
        newId = r.nextInt(100000).toString()
        if (!createdScreenerIds.contains(newId)) {
          createdScreenerIds += newId
          exists = false
        }
      } // end while
      newId
    }

    val recruiterid = {
      var exists = true
      var newId = ""
      while(exists) {
        newId = r.nextInt(100000).toString()
        if (!createdRecruiterIds.contains(newId)) {
          createdRecruiterIds += newId
          exists = false
        }
      } // end while
      newId
    }

    val qualifiedLeadid = {
      var exists = true
      var newId = ""
      while(exists) {
        newId = r.nextInt(100000).toString()
        if (!createdQualifiedLeadIds.contains(newId)) {
          createdQualifiedLeadIds += newId
          exists = false
        }
      } // end while
      newId
    }


    // Get existing ids
    val existingScreenerId = {
      var existingId = "0"
      if (createdScreenerIds.length > 0) {
        val index = r.nextInt(createdScreenerIds.length)
        existingId = createdScreenerIds(index)
      }
      existingId
    }

    val existingRecruiterId = {
      var existingId = "0"
      if (createdRecruiterIds.length > 0) {
        val index = r.nextInt(createdRecruiterIds.length)
        existingId = createdRecruiterIds(index)
      }
      existingId
    }

    val existingQualifiedLeadId = {
      var existingId = "0"
      if (createdQualifiedLeadIds.length > 0) {
        val index = r.nextInt(createdQualifiedLeadIds.length)
        existingId = createdQualifiedLeadIds(index)
      }
      existingId
    }


    val firstName = DataLake.first_nameList(r.nextInt(100))//100
    val lastName = DataLake.last_nameList(r.nextInt(100))//100
    val emailHandle = "@gmail.com"
    val day = r.nextInt(30)+1
    val month = r.nextInt(12)
    val year = (2022)
    val hour = r.nextInt(12)
    val university = DataLake.universityList(r.nextInt(100))//100
    val major = DataLake.majorList(r.nextInt(296))//296
    val email = firstName+lastName+emailHandle
    val home_state = DataLake.stateList(r.nextInt(50))//50
    val pastNoonOrNot = DataLake.pastNoonOrNotList(r.nextInt(2))
    val date = (month.toString()
    +"/"+day.toString()
    +"/"+year.toString())
    val start_time = (hour.toString
    +":"+DataLake.minute(r.nextInt(4))
    +" "+pastNoonOrNot)
    val end_time = ((hour+1).toString
    +":"+DataLake.minute(r.nextInt(4))
    +" "+pastNoonOrNot)
    val contactMethod = DataLake.contactMethodList(r.nextInt(3))//3
    val screenType = DataLake.screenTypeList(r.nextInt(4))//4
    val offerActionDate = (month.toString()
    +"/"+(day+r.nextInt(10)).toString()
    +"/"+year.toString())//They reply sometime within 10 days
    val offerAction = DataLake.response(r.nextInt(3))
    var value = ""



    if(topic == Data.screeners){
     value = (
     screenerid +","+
     firstName+","+
     lastName) 
    }

    if(topic == Data.recruiters){
     value = (
     recruiterid +","+
     firstName+","+
     lastName) 
    }
      

    if(topic == Data.qualifiedLeads){
      value =(
      qualifiedLeadid +","+
      firstName+","+
      lastName+","+
      university+","+
      major+","+
      email+","+
      home_state)
    }

    if(topic == Data.contactAttempts){
      value = (
      existingRecruiterId+","+
      existingQualifiedLeadId+","+
      date+","+
      start_time+","+
      end_time+","+
      contactMethod)
    }


    if(topic == Data.screenings){
      value = (
      existingScreenerId+","+
      existingQualifiedLeadId+","+
      date+","+
      start_time+","+
      end_time+","+
      screenType+","+
      r.nextInt(10).toString+","+   // Number of questions
      r.nextInt(10).toString)       // Number of accepted 
    }


    if(topic == Data.offers){
      value = (
      existingScreenerId+","+
      existingRecruiterId+","+
      existingQualifiedLeadId+","+
      date+","+
      offerActionDate+","+
      contactMethod+","+
      offerAction)
    }

    return value
  } // end valueDeterminer()

  

  def produce(topic: String): String = {

    var value = ""
    var count = 1

    topic match {
        case Data.recruiters => count = 2
        case Data.screeners => count = 1
        case Data.qualifiedLeads => count = 10
        case Data.contactAttempts => count = 8
        case Data.screenings => count = 6
        case Data.offers => count = 3
        case _ => count = 1
    }

    for (x <- 1 to count) {
        val newValue = valueDeterminer(topic)
        value += newValue + "\n"
    }
    
    // Remove the final new line and return
    return value.substring(0, value.length() - 1)

  } // end produce()


} // end object