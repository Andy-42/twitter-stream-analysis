import andy42.twitter.config.Config
import andy42.twitter.config.Config.StreamParametersConfig
import andy42.twitter.decoder.DecodeTransducer.decodeStringToExtract
import andy42.twitter.decoder.Decoder
import andy42.twitter.eventTime.EventTime
import andy42.twitter.output.{SummaryEmitter, WindowSummaryOutput}
import andy42.twitter.summarize.WindowSummarizer.addChunkToSummary
import andy42.twitter.summarize.{EmptyWindowSummaries, WindowSummarizer}
import andy42.twitter.tweet.TweetStream
import zio.clock.Clock
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{ReadError, read}
import zio.duration.Duration
import zio.stream.Transducer.{splitLines, utf8Decode}
import zio.stream.{Sink, ZStream}
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer, stream}

import scala.concurrent.duration.MILLISECONDS

object Test extends zio.App {

  // TODO: Sort out E lineage here - how specific can we make it?
  // TODO: Is it the

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


  type CustomLayer = Has[Config.Service] with TweetStream with Decoder with WindowSummarizer

  val customLayer: ZLayer[Any, Throwable, CustomLayer] =
    configLayer ++ tweetStreamLayer ++ decodeLayer ++ windowSummarizer

  // TODO: Placeholder for now since config is awkward to access this in the stream program
  val config = StreamParametersConfig(
    extractConcurrency = 1,
    chunkSizeLimit = 100,
    chunkGroupTimeout = Duration(100, MILLISECONDS))

  // Get the stream. Since the stream is produced in an effect, it has to be unwrapped.
  val tweetStream: ZStream[TweetStream, Throwable, Byte] =
    ZStream.unwrap {
      for {
        tweetStream <- TweetStream.tweetStream
      } yield tweetStream
    }

  val program: ZStream[CustomLayer with Clock, Throwable, WindowSummaryOutput] =
    tweetStream

      .transduce(utf8Decode >>> splitLines >>> decodeStringToExtract)

      // TODO: Move this into a service so accessing config is easier
      .groupedWithin(
        chunkSize = config.chunkSizeLimit,
        within = config.chunkGroupTimeout)
      .mapAccumM(EmptyWindowSummaries)(addChunkToSummary)
      .flatten

      .tap(x => ZIO.debug(x))

      .catchAll {
        e =>
          println(e) // TODO: Log error
          ZStream()
      }


  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program
      .provideCustomLayer(customLayer)
      .runDrain
      .exitCode
}