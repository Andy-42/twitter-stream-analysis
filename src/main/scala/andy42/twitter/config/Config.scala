package andy42.twitter.config

import andy42.twitter.DurationMillis
import zio.config._
import zio.config.magnolia._
import zio.duration.Duration

object Config {

  val configDescriptor: ConfigDescriptor[Service] =
    descriptor[Service].mapKey(toKebabCase)

  final case class Service(eventTime: EventTimeConfig,
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

  final case class TwitterStreamConfig(sampleApiUrl: String, // TODO: Validate this as an URL
                                       apiKey: String,
                                       apiKeySecret: String,
                                       accessToken: String,
                                       accessTokenSecret: String)
}
