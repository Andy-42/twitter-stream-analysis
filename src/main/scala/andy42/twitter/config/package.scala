package andy42.twitter

import zio.config.typesafe.TypesafeConfigSource
import zio.duration.Duration
import zio.{Has, Layer, UIO, URIO, ZIO, ZLayer}

package object config {

  final case class ConfigTopLevel(eventTime: EventTimeConfig,
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
                                       accessTokenSecret: String,
                                       bufferSize: Int)

  trait Config {
    val eventTime: UIO[EventTimeConfig]
    val streamParameters: UIO[StreamParametersConfig]
    val summaryOutput: UIO[SummaryOutputConfig]
    val twitterStream: UIO[TwitterStreamConfig]
  }

  case class ConfigLive(configTopLevel: ConfigTopLevel) extends Config {
    override val eventTime: UIO[EventTimeConfig] = ZIO.succeed(configTopLevel.eventTime)
    override val streamParameters: UIO[StreamParametersConfig] = ZIO.succeed(configTopLevel.streamParameters)
    override val summaryOutput: UIO[SummaryOutputConfig] = ZIO.succeed(configTopLevel.summaryOutput)
    override val twitterStream: UIO[TwitterStreamConfig] = ZIO.succeed(configTopLevel.twitterStream)
  }

  object ConfigLive {

    import zio.config._
    import zio.config.magnolia._

    implicit val urlDescriptor: Descriptor[org.http4s.Uri] =
      Descriptor[String].transformOrFailLeft(
        s => org.http4s.Uri.fromString(s).swap.map(_.getMessage).swap)(url => url.toString)

    val configDescriptor: ConfigDescriptor[ConfigTopLevel] =
      descriptor[ConfigTopLevel].mapKey(toKebabCase)

    val layer: Layer[ReadError[String], Has[Config]] =
      ZLayer.fromEffect {
        for {
          configTopLevel <- read(configDescriptor from TypesafeConfigSource.fromResourcePath)
        } yield ConfigLive(configTopLevel)
      }
  }

  object Config {
    val eventTime: URIO[Has[Config], EventTimeConfig] = ZIO.serviceWith[Config](_.eventTime)
    val streamParameters: URIO[Has[Config], StreamParametersConfig] = ZIO.serviceWith[Config](_.streamParameters)
    val summaryOutput: URIO[Has[Config], SummaryOutputConfig] = ZIO.serviceWith[Config](_.summaryOutput)
    val twitterStream: URIO[Has[Config], TwitterStreamConfig] = ZIO.serviceWith[Config](_.twitterStream)
  }
}
