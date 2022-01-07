package andy42.twitter

import zio.config._
import zio.config.magnolia._
import zio.duration.Duration

import java.net.URL

object Config {
  final case class Config(eventTime: EventTimeConfig,
                          streamParameters: StreamParametersConfig,
                          summaryOutput: SummaryOutputConfig,
                          twitterStream: TwitterStreamConfig)

  final case class EventTimeConfig(windowSize: Duration,
                                   watermark: Duration) {

    def windowSizeMs: DurationMillis = windowSize.toMillis

    def watermarkMs: DurationMillis = watermark.toMillis
  }

  final case class StreamParametersConfig(extractConcurrency: Int,
                                          chunkSizeLimit: Int,
                                          chunkGroupTimeout: Duration)

  final case class SummaryOutputConfig(topN: Int,
                                       photoDomains: List[String]
                                      ) {
    def isPhotoDomain(domain: String): Boolean = photoDomains.contains(domain)
  }

  final case class TwitterStreamConfig(sampleApiUrl: URL,
                                       apiKey: String,
                                       apiKeySecret: String,
                                       accessToken: String,
                                       accessTokenSecret: String)
}

object ConfigDescriptors {

  import Config._

  val configDescriptor: ConfigDescriptor[Config] =
    descriptor[Config].mapKey(toKebabCase)
}
