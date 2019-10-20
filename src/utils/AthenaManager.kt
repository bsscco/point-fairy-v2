package utils

import kotlinx.coroutines.time.delay
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.*
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable
import java.time.Duration

/**
 * @author bsscco
 * @see <a href="https://docs.aws.amazon.com/ko_kr/athena/latest/ug/code-samples.html">참고 샘플</a>
 */
object AthenaManager {

    data class ProductionReviewRow(
        val userId: Int,
        val cardId: Int,
        val createdAt: String
    )

    suspend fun getRecentProductionReviews(userIds: List<Int>): List<ProductionReviewRow> {
        val client = createAthenaClient()
        try {
            val queryExecutionId = getQueryExecutionId(client, userIds)
            loop@ while (true) {
                val queryExecution = getQueryExecution(client, queryExecutionId)
                when (queryExecution.status().state()) {
                    QueryExecutionState.FAILED -> throw RuntimeException("Query Failed to run with Error Message: " + queryExecution.status().stateChangeReason())
                    QueryExecutionState.CANCELLED -> throw RuntimeException("Query was cancelled.")
                    QueryExecutionState.SUCCEEDED -> break@loop
                    else -> delay(Duration.ofSeconds(1)) // Sleep an amount of time before retrying again.
                }
            }
            return convertToProdReviewRows(getQueryResults(client, queryExecutionId))
        } catch (e: Exception) {
            throw e
        } finally {
            client.close()
        }
    }

    private fun createAthenaClient(): AthenaClient {
        return AthenaClient.builder()
            .region(Region.AP_NORTHEAST_2)
            .credentialsProvider {
                object : AwsCredentials {
                    override fun accessKeyId() = Config.ATHENA_ACCESS_KEY_ID

                    override fun secretAccessKey() = Config.ATHENA_SECRET_ACCESS_KEY
                }
            }
            .build()
    }

    private fun getQueryExecutionId(client: AthenaClient, userIds: List<Int>): String {
        @Suppress("SimpleRedundantLet")
        return createStartQueryExecutionRequest(userIds)
            .let { request -> client.startQueryExecution(request) }
            .let { response -> response.queryExecutionId() }
    }

    private fun createStartQueryExecutionRequest(userIds: List<Int>): StartQueryExecutionRequest {
        return StartQueryExecutionRequest.builder()
            .queryString(Config.getRecentProductionReviewsQuery(userIds))
            .queryExecutionContext(QueryExecutionContext.builder().database(Config.ATHENA_DB_NAME).build())
            .resultConfiguration(ResultConfiguration.builder().outputLocation(Config.ATHENA_OUTPUT_LOCATION).build())
            .build()
    }

    private fun getQueryExecution(client: AthenaClient, queryExecutionId: String): QueryExecution {
        return client.getQueryExecution(createQueryExecutionRequest(queryExecutionId)).queryExecution()
    }

    private fun createQueryExecutionRequest(queryExecutionId: String?): GetQueryExecutionRequest {
        return GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build()
    }

    private fun getQueryResults(client: AthenaClient, queryExecutionId: String): GetQueryResultsIterable {
        return client.getQueryResultsPaginator(createQueryResultsRequest(queryExecutionId))
    }

    private fun createQueryResultsRequest(queryExecutionId: String): GetQueryResultsRequest {
        return GetQueryResultsRequest.builder().queryExecutionId(queryExecutionId).build()
    }

    private fun convertToProdReviewRows(queryResults: GetQueryResultsIterable): List<ProductionReviewRow> {
        return ArrayList<ProductionReviewRow>().apply {
            for (result in queryResults) {
                result.resultSet().rows().forEachIndexed { index, row ->
                    if (index >= 1) { // 첫 행은 컬럼명
                        add(
                            ProductionReviewRow(
                                row.data()[0].varCharValue().toInt(),
                                row.data()[1].varCharValue().toInt(),
                                row.data()[2].varCharValue()
                            )
                        )
                    }
                }
            }
        }
    }
}