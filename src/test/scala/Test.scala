import andy42.twitter.config.Config
import andy42.twitter.decoder.CreateTweet.isCreateTweet
import andy42.twitter.decoder.Decoder
import andy42.twitter.eventTime.EventTime
import andy42.twitter.tweet.TweetStream
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{ReadError, read}
import zio.console._
import zio.stream.Transducer.{splitLines, utf8Decode}
import zio.stream.ZStream
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}

object Test extends zio.App {

  // TODO: Sort out E lineage here - how specific can we make it?

  val configLayer: ZLayer[Any, ReadError[String], Has[Config.Service]] =
    read(Config.configDescriptor from TypesafeConfigSource.fromResourcePath).toLayer

  val tweetStreamLayer: ZLayer[Any, Throwable, TweetStream] =
    configLayer >>> TweetStream.live

  val eventTimeLayer: ZLayer[Any, ReadError[String], EventTime] =
    configLayer >>> EventTime.live

  val decodeLayer: ZLayer[Any, Throwable, Decoder] =
    (configLayer ++ eventTimeLayer) >>> Decoder.live

  val env: ZLayer[Any, Throwable, Environment] =
    tweetStreamLayer ++ Console.live ++ decodeLayer

  type Environment = Console with TweetStream with Decoder

  val program: ZIO[Environment, Throwable, Unit] =
    TweetStream.tweetStream.flatMap { bytes =>
      bytes
        .transduce(utf8Decode >>> splitLines)
        .filter(isCreateTweet)

        .tap(line => ZIO.debug(line))

        // TODO: implement as a transducer
        .mapM { line =>
          Decoder.decodeLineToExtract(line).fold(
            _ => ZStream.empty, // TODO: Log, or split into two streams
            ZStream(_)
          )
        }.flatten

        .tap(extract => ZIO.debug(extract.toString))

        .catchAll { // TODO: Log e
          e =>
            println(e)
            ZStream()
        }
        .runDrain
    }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program
      .provideLayer(env)
      .exitCode
}