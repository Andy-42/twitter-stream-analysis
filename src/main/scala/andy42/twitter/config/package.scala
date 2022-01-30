package andy42.twitter

import zio.config.typesafe.TypesafeConfigSource
import zio.duration.Duration
import zio.{Has, Layer, UIO, URIO, ZIO, ZLayer}

package object config {

  final case class Config(eventTime: EventTimeConfig,
                          streamParameters: StreamParametersConfig,
                          summaryOutput: SummaryOutputConfig,
                          twitterStream: TwitterStreamConfig) {

    def streamParametersM: UIO[StreamParametersConfig] = ZIO.succeed(streamParameters)
  }

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
                                       accessTokenSecret: String,
                                       bufferSize: Int)

  object ConfigLive {

    import zio.config._
    import zio.config.magnolia._

    implicit val urlDescriptor: Descriptor[org.http4s.Uri] =
      Descriptor[String].transformOrFailLeft(
        s => org.http4s.Uri.fromString(s).swap.map(_.getMessage).swap)(url => url.toString)

    val configDescriptor: ConfigDescriptor[Config] =
      descriptor[Config].mapKey(toKebabCase)

    val layer: ZLayer[Any, ReadError[String], Has[Config]] =
      read(configDescriptor from TypesafeConfigSource.fromResourcePath).toLayer
  }

  object Config {
    /**
     * Access the stream configuration.
     * This is only needed if the configuration has to be accessed from outside a Layer.
     * The member being accessed has to be an effect in order to create an accessor.
     */
    def streamParameters: URIO[Has[Config], StreamParametersConfig] =
      ZIO.serviceWith[Config](_.streamParametersM)
  }
}
