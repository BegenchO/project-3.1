package project3

object Data {

    // People

    // id, first_name, last_name
    val screeners = "screeners"
    val screenersFields = List("screener_id","first_name", "last_name")

    // id, first_name, last_name
    val recruiters = "recruiters"
    val recruitersFields = List("recruiter_id","first_name", "last_name")

    // id,first_name, last_name, university, major, email, home_state
    val qualifiedLeads = "qualifiedLeads"
    val qualifiedLeadsFields = List("ql_id","first_name", "last_name", "university", "major", "email", "home_state")



    // Actions

    // recruiter_id, ql_id, contact_date, start_time, end_time, contact_method
    val contactAttempts = "contactAttempts"
    val contactAttemptsFields = List("recruiter_id", "ql_id", "contact_date", "start_time", "end_time", "contact_method")

    // screener_id, ql_id, screening_date, start_time, end_time, screening_type, question_number, question_accepted
    val screenings = "screenings"
    val screeningsFields = List("screener_id", "ql_id", "screening_date", "start_time", "end_time", "screening_type", "question_number", "question_accepted")

    // screener_id, recruiter_id, ql_id, offer_extended_date, offer_action_date, contact_method, offer_action
    val offers = "offers"
    val offersFields = List("screener_id", "recruiter_id", "ql_id", "offer_extended_date", "offer_action_date", "contact_method", "offer_action")



    // List of topics
    val topics = List(screeners, recruiters, qualifiedLeads, contactAttempts, screenings, offers)




    // Methods 

    // Create data (credit to Theodore)
    def fetchData(topic: String): String = {
        DataGenerator.produce(topic)
    } // end fetchData



    // Fetch data from Mockaroo API
    def fetchDataAPI(topic: String): String = {
        var result = ""
        try {
            var url = s"https://my.api.mockaroo.com/${topic}?key=9e6718b0"
            result = scala.io.Source.fromURL(url).mkString
        } catch {
            case e: Exception => println("Error fetching, continuing...")
        }
        result
               
    } // end fetchDataAPI


    

} // end object