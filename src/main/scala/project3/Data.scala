package project3

object Data {

    // People

    // id, first_name, last_name
    val screeners = "screeners"

    // id, first_name, last_name
    val recruiters = "recruiters"

    // id,first_name, last_name, university, major, email, home_state
    val qualifiedLeads = "qualifiedLeads"



    // Actions

    // recruiter_id, ql_id, contact_date, start_time, end_time, contact_method
    val contactAttempts = "contactAttempts"

    // screener_id, ql_id, screening_date, start_time, end_time, screening_type, question_number, question_accepted
    val screenings = "screenings"

    // screener_id, recruiter_id, ql_id, offer_extended_date, offer_action_date, contact_method, offer_action
    val offers = "offers"



    // List of topics
    val topics = List(screeners, recruiters, qualifiedLeads, contactAttempts, screenings, offers)




    // Methods 



    // Create data (credit to Theodore)
    def fetchData(topic: String): String = {
        DataGenerator.produce(topic)
    } // end fetchData



    // Fetch data from Mockaroo API
    def fetchDataAPI(dataType: String): String = {
        var result = ""
        try {
            var url = s"https://my.api.mockaroo.com/${dataType}?key=9e6718b0"
            result = scala.io.Source.fromURL(url).mkString
        } catch {
            case e: Exception => println("Error fetching, continuing...")
        }
        result
               
    } // end fetchDataAPI


    

} // end object