package utils

import com.google.api.services.sheets.v4.SheetsScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ServiceAccountCredentials
import io.ktor.client.HttpClient
import io.ktor.client.request.*
import io.ktor.client.response.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.content.TextContent
import java.io.FileInputStream
import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class SpreadSheetsManager(private val httpClient: HttpClient) {

    enum class StatusValue { NONE, FIRST_DELIVERY, WARNING, BAN_POINT, }

    data class StatusRow(
        val rowIndex: Int,
        val nickname: String,
        val userId: Int,
        val status: String,
        val updatedAt: String
    )

    private data class GetAllStatusRowsResponse(val values: List<List<String>>)

    private val googleCredentials = createGoogleCredentials()
        get() = field.also { it.refreshIfExpired() }

    private fun createGoogleCredentials(): GoogleCredentials {
        @Suppress("SimpleRedundantLet")
        return FileInputStream(Config.GOOGLE_SERVICE_ACCOUNT_PATH)
            .let { stream -> ServiceAccountCredentials.fromStream(stream) }
            .let { serviceAccountCredentials ->
                serviceAccountCredentials.createScoped(
                    Collections.singletonList(
                        SheetsScopes.SPREADSHEETS
                    )
                )
            }
    }

    suspend fun getDangerStatusRows(): List<StatusRow> {
        return httpClient.use { client ->
            val response = client.get<GetAllStatusRowsResponse>(getAllStatusRowsUrl()) { setRequestHeader() }
            filterOnlyDangerStatus(convertToStatusRows(response))
        }
    }

    private fun getAllStatusRowsUrl(): String {
        val encodedSheetsId = toUrlEncoded(Config.SHEETS_ID)
        val encodedSheetRange = toUrlEncoded(Config.STATUS_SHEET_RANGE)
        return "https://sheets.googleapis.com/v4/spreadsheets/$encodedSheetsId/values/$encodedSheetRange"
    }

    private fun toUrlEncoded(string: String) = URLEncoder.encode(string, "UTF-8")

    private fun HttpRequestBuilder.setRequestHeader() {
        header("Authorization", "Bearer ${googleCredentials.accessToken.tokenValue}")
    }

    private fun convertToStatusRows(response: GetAllStatusRowsResponse): List<StatusRow> {
        return response.values.mapIndexed { index, value -> convertToStatusRow(index, value) }
    }

    private fun convertToStatusRow(rowIndex: Int, rawRow: List<String>): StatusRow {
        return StatusRow(rowIndex, rawRow[0], rawRow[1].toInt(), rawRow[2], rawRow[3])
    }

    private fun filterOnlyDangerStatus(statusRows: List<StatusRow>): List<StatusRow> {
        return statusRows.filter { it.status != StatusValue.NONE.name }
    }

    private fun getCurrentDateString(): String {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .let { LocalDateTime.now().format(it) }
    }

    suspend fun changeStatusToNone(rowIndex: Int) {
        httpClient.use { client ->
            client.put<HttpResponse>(getStatusToNoneUrl(rowIndex)) {
                setRequestHeader()
                body = TextContent(
                    contentType = ContentType.Application.Json,
                    text = createNewStatusRowValue(StatusValue.NONE)
                )
            }
        }
    }

    private fun getStatusToNoneUrl(rowIndex: Int): String {
        val encodedSheetsId = toUrlEncoded(Config.SHEETS_ID)
        val encodedSheetRange = toUrlEncoded(Config.getCardSheetStatusUpdateRange(rowIndex))
        return "https://sheets.googleapis.com/v4/spreadsheets/$encodedSheetsId/values/$encodedSheetRange?valueInputOption=USER_ENTERED"
    }

    private fun createNewStatusRowValue(statusValue: StatusValue): String {
        return """
            {
                values: [["${statusValue.name}", "${getCurrentDateString()}"]]
            }
        """
    }
}