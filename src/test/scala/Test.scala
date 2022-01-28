import andy42.twitter.config.{Config, ConfigLive}
import andy42.twitter.decoder.DecodeTransducer.decodeStringToExtract
import andy42.twitter.decoder.{Decoder, DecoderLive}
import andy42.twitter.eventTime.{EventTime, EventTimeLive}
import andy42.twitter.output.{SummaryEmitter, SummaryEmitterLive, WindowSummaryOutput}
import andy42.twitter.summarize.SummarizeWindowTransducer.summarizeChunks
import andy42.twitter.summarize.WindowSummarizer.addChunkToSummary
import andy42.twitter.summarize.{EmptyWindowSummaries, SummarizeWindowTransducer, WindowSummarizer, WindowSummarizerLive}
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

  val tweetSummaryProgram: ZIO[Has[Config], Nothing, ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput]] =
    for {
      config <- Config.config
    } yield tweetStream
      // TODO: If these transducers are combined, it confuses the presentation compiler
      .transduce(utf8Decode >>> splitLines)
      .transduce(decodeStringToExtract)

      // This implementation uses `mapAccumM` to statefully summarize the extract stream.
      // The `groupedWithin` behaviour should be reproduced for the transducer case below.
      // Note that groupedWithin also changes the stream from O to Chunk[O]

      .groupedWithin(
        chunkSize = config.streamParameters.chunkSizeLimit,
        within = config.streamParameters.chunkGroupTimeout)
      .mapAccumM(EmptyWindowSummaries)(addChunkToSummary)
      .flatten

      // The transducer implementation is certainly more concise,
      // and it doesn't need the extra `flatten` needed with the `mapAccumM` implementation.
      // The semantics of groupedWithin(chunkSize, within) needs to be implemented.
//      .transduce(summarizeChunks)

      .mapM(SummaryEmitter.emitSummary)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    tweetSummaryProgram.provideCustomLayer(configLayer).flatMap {
      _.provideCustomLayer(customLayer)
        .tap(ZIO.debug(_))
        .runDrain
        .exitCode
    }
}
