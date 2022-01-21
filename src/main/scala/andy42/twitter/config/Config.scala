package andy42.twitter.config

import zio.config._
import zio.config.magnolia._
import zio.duration.Duration

object Config {

  implicit val urlDescriptor: Descriptor[org.http4s.Uri] =
    Descriptor[String].transformOrFailLeft(
      s => org.http4s.Uri.fromString(s).swap.map(_.getMessage).swap)(url => url.toString)

  val configDescriptor: ConfigDescriptor[Service] =
    descriptor[Service].mapKey(toKebabCase)

  final case class Service(eventTime: EventTimeConfig,
                           streamParameters: StreamParametersConfig,
                           summaryOutput: SummaryOutputConfig,
                           twitterStream: TwitterStreamConfig)

  final case class EventTimeConfig(windowSize: Duration,
                                   watermark: Duration)

  final case class StreamParametersConfig(extractConcurrency: Int,
                                          chunkSizeLimit: Int,
                                          chunkGroupTimeout: Duration)

  final case class SummaryOutputConfig(topN: Int,
                                       photoDomains: List[String]
                                      ) {
    def isPhotoDomain(domain: String): Boolean = photoDomains.contains(domain)
  }


  final case class TwitterStreamConfig(sampleApiUrl: org.http4s.Uri,
                                       apiKey: String,
                                       apiKeySecret: String,
                                       accessToken: String,
                                       accessTokenSecret: String)
}
