import andy42.twitter.config.{Config, ConfigLive}
import andy42.twitter.decoder.{DecodeTransducer, Decoder, DecoderLive}
import andy42.twitter.eventTime.{EventTime, EventTimeLive}
import andy42.twitter.output.{SummaryEmitter, SummaryEmitterLive, WindowSummaryOutput}
import andy42.twitter.summarize.SummarizeWindowTransducer.summarizeChunks
import andy42.twitter.summarize.WindowSummarizer.addChunkToSummary
import andy42.twitter.summarize.{EmptyWindowSummaries, WindowSummarizer, WindowSummarizerLive}
import andy42.twitter.tweet.{TweetStream, TweetStreamLive}
import zio.clock.Clock
import zio.stream.Transducer.{splitLines, utf8Decode}
import zio.stream.ZStream
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}

object Test extends zio.App {

  // Reading the config could produce a ReadError[Sting], but fail right here if that happens.
  val configLayer: ZLayer[Any, Nothing, Has[Config]] =
    ConfigLive.layer.orDie

  val tweetStreamLayer: ZLayer[Any, Nothing, Has[TweetStream]] =
    configLayer >>> TweetStreamLive.layer

  val eventTimeLayer: ZLayer[Any, Nothing, Has[EventTime]] =
    configLayer >>> EventTimeLive.layer

  val decodeLayer: ZLayer[Any, Nothing, Has[Decoder]] =
    (configLayer ++ eventTimeLayer) >>> DecoderLive.layer

  val summaryEmitterLayer: ZLayer[Any, Nothing, Has[SummaryEmitter]] =
    (configLayer ++ eventTimeLayer) >>> SummaryEmitterLive.layer

  val windowSummarizer: ZLayer[Any, Nothing, Has[WindowSummarizer]] =
    (Clock.live ++ eventTimeLayer ++ summaryEmitterLayer) >>> WindowSummarizerLive.layer

  type CustomLayer = Has[TweetStream] with Has[Decoder] with Has[WindowSummarizer] with Has[SummaryEmitter] with Has[EventTime]
  val customLayer = tweetStreamLayer ++ decodeLayer ++ windowSummarizer ++ summaryEmitterLayer ++ eventTimeLayer


  // Get the stream. Since the stream is produced in an effect, it has to be unwrapped.
  val tweetStream: ZStream[Has[TweetStream], Nothing, Byte] = ZStream.unwrap {
    for {
      stream <- TweetStream.tweetStream
    } yield stream
      // The http4s stream can fail with a Throwable.
      // In a real application, we would have a more meaningful way to recover the stream
      // (e.g., reopen it). This should also be aligned with managing the HTTP client.
      // For now, just print the error and stop.
      .catchAll {
        e =>
          println(e) // TODO: Log error
          ZStream()
      }
  }

  val tweetSummaryProgram1: ZIO[Has[Config] with Has[Decoder], Nothing, ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput]] =
    for {
      decodeStringToExtract <- DecodeTransducer.decodeStringToExtract
    } yield tweetStream
      // TODO: If these transducers are combined, it confuses the presentation compiler
      .transduce(utf8Decode >>> splitLines)
      .transduce(decodeStringToExtract)

      .transduce(summarizeChunks)

      .mapM(SummaryEmitter.emitSummary)

  val tweetSummaryProgram2: ZIO[Has[Config] with Has[Decoder], Nothing, ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput]] =
    for {
      streamParameters <- Config.streamParameters
      decodeStringToExtract <- DecodeTransducer.decodeStringToExtract
    } yield tweetStream
      // TODO: If these transducers are combined, it confuses the presentation compiler
      .transduce(utf8Decode >>> splitLines)
      .transduce(decodeStringToExtract)

      .groupedWithin(
        chunkSize = streamParameters.chunkSizeLimit,
        within = streamParameters.chunkGroupTimeout)
      .mapAccumM(EmptyWindowSummaries)(addChunkToSummary)
      .flatten

      .mapM(SummaryEmitter.emitSummary)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    tweetSummaryProgram1.provideCustomLayer(configLayer ++ decodeLayer).flatMap {
      _.provideCustomLayer(customLayer)
        .tap(ZIO.debug(_))
        .runDrain
        .exitCode
    }
}
