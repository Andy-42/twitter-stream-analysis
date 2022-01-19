import andy42.twitter.config.Config
import andy42.twitter.decoder.CreateTweet.isCreateTweet
import andy42.twitter.decoder.Decoder
import andy42.twitter.decoder.Decoder.decodeLineToExtract
import andy42.twitter.eventTime.EventTime
import andy42.twitter.output.{SummaryEmitter, WindowSummaryOutput}
import andy42.twitter.summarize.WindowSummarizer.addChunkToSummary
import andy42.twitter.summarize.{EmptyWindowSummaries, WindowSummaries, WindowSummarizer}
import andy42.twitter.tweet.TweetStream
import zio.clock.Clock
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{ReadError, read}
import zio.duration.Duration
import zio.stream.Transducer.{splitLines, utf8Decode}
import zio.stream.{UStream, ZStream}
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

  val summaryEmitterLayer: ZLayer[Any, Throwable, SummaryEmitter] =
    (configLayer ++ eventTimeLayer) >>> SummaryEmitter.live

  val windowSummarizer: ZLayer[Any, Throwable, WindowSummarizer] =
    (Clock.live ++ eventTimeLayer ++ summaryEmitterLayer) >>> WindowSummarizer.live

  // TODO: Why do we need Clock in the top-level environment?
  val env: ZLayer[Any, Throwable, Environment] =
     Clock.live ++ tweetStreamLayer ++ decodeLayer ++ windowSummarizer

  type Environment = TweetStream with Decoder with WindowSummarizer with Clock

  val program: ZIO[Environment, Throwable, Unit] =
      TweetStream.tweetStream.flatMap { bytes =>
        bytes
          .transduce(utf8Decode >>> splitLines)
          // .transduce(utf8Decode >>> splitLines >>> decodeStringToExtract >>> summarizeByWindow)

          // TODO: Move it to a Transducer - decodeStringToExtract
          .filter(isCreateTweet)
          .mapM(decodeLineToExtract)
          .collect {
            case Right(extract) => extract
            // TODO: Log the Lefts
          }

          // TODO: Move it to a Transducer - summarizeByWindow
          .groupedWithin(chunkSize = 10000, within = Duration.fromMillis(100)) // TODO: Config
          .mapAccumM(EmptyWindowSummaries)(addChunkToSummary)
          .flatten

          .tap(x => ZIO.debug(x))

          .catchAll {
            e =>
              println(e) // TODO: Log error
              ZStream()
          }
          .runDrain
      }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program
      .provideLayer(env)
      .exitCode
}