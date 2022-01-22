import andy42.twitter.config.{Config, ConfigLive, StreamParametersConfig}
import andy42.twitter.decoder.DecodeTransducer.decodeStringToExtract
import andy42.twitter.decoder.{Decoder, DecoderLive}
import andy42.twitter.eventTime.{EventTime, EventTimeLive}
import andy42.twitter.output.{SummaryEmitter, SummaryEmitterLive, WindowSummaryOutput}
import andy42.twitter.summarize.WindowSummarizer.addChunkToSummary
import andy42.twitter.summarize.{EmptyWindowSummaries, WindowSummarizer, WindowSummarizerLive}
import andy42.twitter.tweet.{TweetStream, TweetStreamLive}
import zio.clock.Clock
import zio.config.ReadError
import zio.duration.Duration
import zio.stream.Transducer.{splitLines, utf8Decode}
import zio.stream.ZStream
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}

import scala.concurrent.duration.MILLISECONDS

object Test extends zio.App {

  // TODO: Sort out E lineage here - how specific can we make it?
  // TODO: Is it the

  // ReadError[String] - from config
  // Throwable - http4s Stream
  // ParseResult[Uri] from validating URL in config

  val configLayer: ZLayer[Any, ReadError[String], Has[Config]] =
    ConfigLive.layer

  val tweetStreamLayer: ZLayer[Any, ReadError[String], Has[TweetStream]] =
    configLayer >>> TweetStreamLive.layer

  val eventTimeLayer: ZLayer[Any, ReadError[String], Has[EventTime]] =
    configLayer >>> EventTimeLive.layer

  val decodeLayer: ZLayer[Any, ReadError[String], Has[Decoder]] =
    (configLayer ++ eventTimeLayer) >>> DecoderLive.layer

  val summaryEmitterLayer: ZLayer[Any, ReadError[String], Has[SummaryEmitter]] =
    (configLayer ++ eventTimeLayer) >>> SummaryEmitterLive.layer

  // TODO: Probably should leave Clock in the environment at this point?
  val windowSummarizer: ZLayer[Any, ReadError[String], Has[WindowSummarizer]] =
    (Clock.live ++ eventTimeLayer ++ summaryEmitterLayer) >>> WindowSummarizerLive.layer


  type CustomLayer = Has[TweetStream] with Has[Decoder] with Has[WindowSummarizer] with Has[SummaryEmitter]

  val customLayer = tweetStreamLayer ++ decodeLayer ++ windowSummarizer ++ summaryEmitterLayer

  // TODO: Placeholder for now since config is awkward to access this in the stream program
  val config = StreamParametersConfig(
    extractConcurrency = 1,
    chunkSizeLimit = 100,
    chunkGroupTimeout = Duration(100, MILLISECONDS))

  // Get the stream. Since the stream is produced in an effect, it has to be unwrapped.
  // FIXME: Where is this NoSuchElementException coming from?
  val tweetStream: ZStream[Has[TweetStream], Nothing, Byte] =
  ZStream.unwrap {
    for {
      stream <- TweetStream.tweetStream
    } yield stream
      // In a real application, we would have a more meaningful way to recover the stream
      // (e.g., reopen it). This should also be aligned with managing the HTTP client.
      // For now, just print the error and stop.
      .catchAll {
        e =>
          println(e) // TODO: Log error
          ZStream()
      }
  }


  // FIXME: What is this E1 error in the stream? Where is it coming from?
  val program: ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput] =
    tweetStream
      // TODO: Combining the following transducers doesn't work because of the error context (?).
      // TODO: Specifying them in two separate `transduce` calls does work though?
      //      .transduce(utf8Decode >>> splitLines >>> decodeStringToExtract)
      .transduce(utf8Decode >>> splitLines)
      .transduce(decodeStringToExtract)

      .groupedWithin(
        chunkSize = config.chunkSizeLimit,
        within = config.chunkGroupTimeout)

      .mapAccumM(EmptyWindowSummaries)(addChunkToSummary)
      .flatten

      .mapM(SummaryEmitter.emitSummary)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    program.provideCustomLayer(customLayer)
      .tap(x => ZIO.debug(x))
      .runDrain
      .exitCode
}
