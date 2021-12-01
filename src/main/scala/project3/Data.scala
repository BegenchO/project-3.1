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

    // recruiter_id, ql_id, contact_date, start_time, end_time, contact_metho
    private val contactAttempts = "contactAttempts"

    // screener_id, ql_id, screening_date, start_time, end_time, screening_type, question_number, question_accepted
    private val screening = "screening"

    // screener_id, recruiter_id, ql_id, offer_extended_date, offer_action_date, contact_method, offer_action
    private val offers = "offers"



    // Methods 


    // Fetch data from Mockaroo API
    def fetchData(dataType: String): String = {
        var url = s"https://my.api.mockaroo.com/${dataType}?key=9e6718b0"
        val result = scala.io.Source.fromURL(url).mkString
        result
    } // end fetchData

} // end class