package andy42.twitter.config

import zio.IO
import zio.config._
import zio.config.typesafe._
import zio.duration._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object ConfigSpec extends DefaultRunnableSpec {

  def spec: Spec[TestEnvironment, TestFailure[ReadError[String]], TestSuccess] = suite("ConfigSpec") {

    val validHoconString =
      """
        |twitter-stream {
        |  sample-api-url = "https://stream.twitter.com/1.1/statuses/sample.json"
        |
        |  api-key = API_KEY
        |  api-key-secret = API_KEY_SECRET
        |  access-token = ACCESS_TOKEN
        |  access-token-secret = ACCESS_TOKEN_SECRET
        |}
        |
        |event-time {
        |  window-size = "5 seconds"
        |  watermark = "15 seconds"
        |}
        |
        |stream-parameters {
        |  extract-concurrency = 2
        |  chunk-size-limit = 10000
        |  chunk-group-timeout = "1 second"
        |}
        |
        |summary-output {
        |  top-n = 10
        |  photo-domains = [ "www.instagram.com", "pic.twitter.com" ]
        |}
        |""".stripMargin

    val expected = ConfigTopLevel(
      eventTime = EventTimeConfig(
        windowSize = 5.seconds,
        watermark = 15.seconds),
      streamParameters = StreamParametersConfig(
        extractConcurrency = 2,
        chunkSizeLimit = 10000,
        chunkGroupTimeout = 1.seconds),
      summaryOutput = SummaryOutputConfig(
        topN = 10,
        photoDomains = List("www.instagram.com", "pic.twitter.com")
      ),
      twitterStream = TwitterStreamConfig(
        sampleApiUrl = org.http4s.Uri.unsafeFromString("https://stream.twitter.com/1.1/statuses/sample.json"),
        apiKey = "API_KEY",
        apiKeySecret = "API_KEY_SECRET",
        accessToken = "ACCESS_TOKEN",
        accessTokenSecret = "ACCESS_TOKEN_SECRET")
    )

    val result: IO[ReadError[String], Config] =
      read(ConfigLive.configDescriptor from ConfigSource.fromHoconString(validHoconString))

    testM("A valid configuration can be read") {
      assertM(result)(equalTo(expected))
    }
  }
}
