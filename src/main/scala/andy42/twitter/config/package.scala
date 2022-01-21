package andy42.twitter

import zio.config.typesafe.TypesafeConfigSource
import zio.duration.Duration
import zio.{Has, ZLayer}

package object config {

  trait Config {
    def eventTime: EventTimeConfig

    def streamParameters: StreamParametersConfig

    def summaryOutput: SummaryOutputConfig

    def twitterStream: TwitterStreamConfig
  }

  final case class ConfigTopLevel(eventTime: EventTimeConfig,
                                  streamParameters: StreamParametersConfig,
                                  summaryOutput: SummaryOutputConfig,
                                  twitterStream: TwitterStreamConfig) extends Config

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

  object ConfigLive {

    import zio.config._
    import zio.config.magnolia._

    implicit val urlDescriptor: Descriptor[org.http4s.Uri] =
      Descriptor[String].transformOrFailLeft(
        s => org.http4s.Uri.fromString(s).swap.map(_.getMessage).swap)(url => url.toString)

    val configDescriptor: ConfigDescriptor[ConfigTopLevel] =
      descriptor[ConfigTopLevel].mapKey(toKebabCase)

    val layer: ZLayer[Any, ReadError[String], Has[Config]] =
      read(configDescriptor from TypesafeConfigSource.fromResourcePath).toLayer
  }
}
