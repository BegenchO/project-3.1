package project3

object Data {

    // People
    private val screeners = "screeners"
    private val recruiters = "recruiters"
    private val qualifiedLead = "qualifiedLead"

    // Actions
    private val contactAttempts = "contactAttempts"
    private val screening = "screening"
    private val offers = "offers"

    def getScreeners(): String = {
        return fetchData(screeners)
    }

    private def fetchData(dataType: String): String = {
        var url = s"https://my.api.mockaroo.com/${dataType}?key=9e6718b0"
        val result = scala.io.Source.fromURL(url).mkString
        result
    } // end fetchData

} // end class