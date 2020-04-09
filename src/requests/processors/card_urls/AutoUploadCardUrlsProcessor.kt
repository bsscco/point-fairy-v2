package requests.processors.card_urls

import io.ktor.application.ApplicationCall
import io.ktor.response.respond
import requests.processors.RequestProcessor
import utils.AthenaManager
import utils.AthenaManager.ProductionReviewRow
import utils.HttpClientManager
import utils.SpreadSheetsManager
import utils.SpreadSheetsManager.StatusRow

class AutoUploadCardUrlsProcessor(call: ApplicationCall) : RequestProcessor(call) {

    data class StatusProdReviewPair(
        val statusRow: StatusRow,
        val productionReviewRow: ProductionReviewRow
    )

    private val httpClient = HttpClientManager.createClient()
    private val sheetsManager = SpreadSheetsManager(httpClient)

    override suspend fun process() {
        try {
            getNewStatusProdReviewPairs().let {
                changeStatusesToNone(it)
            }

        } catch (e: Exception) {
            println(e)
        }
        call.respond("OK")
    }

    private suspend fun getNewStatusProdReviewPairs(): List<StatusProdReviewPair> {
        @Suppress("ComplexRedundantLet")
        return sheetsManager.getDangerStatusRows()
            .let { statusRows -> mapStatusesAndProdReviews(statusRows, getRecentProdReviews(statusRows)) }
            .let { statusReviewRows -> filterByNewerThanStatusUpdatedDate(statusReviewRows) }
    }

    private suspend fun getRecentProdReviews(statusRows: List<StatusRow>): List<ProductionReviewRow> {
        return AthenaManager.getRecentProductionReviews(statusRows.map { it.userId })
    }

    private fun mapStatusesAndProdReviews(
        statusRows: List<StatusRow>,
        prodReviewRows: List<ProductionReviewRow>
    ): List<StatusProdReviewPair> {
        return statusRows.mapNotNull { statusRow -> statusRow.createOrNullStatusProdReviewPair(prodReviewRows) }
    }

    private fun StatusRow.createOrNullStatusProdReviewPair(prodReviewRows: List<ProductionReviewRow>): StatusProdReviewPair? {
        return prodReviewRows.findNewest(userId)?.let { prodReviewRow -> StatusProdReviewPair(this, prodReviewRow) }
    }

    private fun List<ProductionReviewRow>.findNewest(userId: Int): ProductionReviewRow? {
        return firstOrNull { prodReviewRow -> prodReviewRow.userId == userId }
    }

    private fun filterByNewerThanStatusUpdatedDate(statusProdReviewRows: List<StatusProdReviewPair>): List<StatusProdReviewPair> {
        return statusProdReviewRows.filter { statusProdReview -> statusProdReview.productionReviewRow.createdAt >= statusProdReview.statusRow.updatedAt }
    }

    private suspend fun changeStatusesToNone(statusReviewPairs: List<StatusProdReviewPair>) {
        statusReviewPairs.forEach { sheetsManager.changeStatusToNone(it.statusRow.rowIndex) }
    }
}