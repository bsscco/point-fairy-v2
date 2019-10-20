package requests.processors.card_urls

import io.ktor.application.call
import io.ktor.routing.Route
import io.ktor.routing.post

fun Route.cardUrls() {
    post("/card_urls/auto_upload") { AutoUploadCardUrlsProcessor(call).process() }
}