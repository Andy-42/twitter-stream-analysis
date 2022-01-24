package andy42.twitter

import zio.config.typesafe.TypesafeConfigSource
import zio.duration.Duration
import zio.{Has, IO, ZIO, ZLayer}

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
                                       accessTokenSecret: String)

  trait Config {
    val configTopLevel: ConfigTopLevel
    val configTopLevelM: ZIO[Any, Nothing, ConfigTopLevel]
  }

  case class ConfigLive(configTopLevel: ConfigTopLevel) extends Config {
    override val configTopLevelM: ZIO[Any, Nothing, ConfigTopLevel] = ZIO.succeed(configTopLevel)
  }

  object ConfigLive {

    import zio.config._
    import zio.config.magnolia._

    implicit val urlDescriptor: Descriptor[org.http4s.Uri] =
      Descriptor[String].transformOrFailLeft(
        s => org.http4s.Uri.fromString(s).swap.map(_.getMessage).swap)(url => url.toString)

    val configDescriptor: ConfigDescriptor[ConfigTopLevel] =
      descriptor[ConfigTopLevel].mapKey(toKebabCase)

    val layer: ZLayer[Any, ReadError[String], Has[Config]] =
      ZLayer.fromEffect {
        for {
          configTopLevel <- read(configDescriptor from TypesafeConfigSource.fromResourcePath)
        } yield ConfigLive(configTopLevel)
      }
  }

  object Config {
    val config: ZIO[Has[Config], Nothing, ConfigTopLevel] = ZIO.serviceWith[Config](_.configTopLevelM)
  }
}
