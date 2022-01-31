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
import zio.{ExitCode, Has, ULayer, URIO, ZEnv, ZIO}

object Main extends zio.App {

  // Reading the config could produce a ReadError[Sting], but fail right here if that happens.
  val configLayer: ULayer[Has[Config]] =
    ConfigLive.layer.orDie

  val tweetStreamLayer: ULayer[Has[TweetStream]] =
    configLayer >>> TweetStreamLive.layer

  val eventTimeLayer: ULayer[Has[EventTime]] =
    configLayer >>> EventTimeLive.layer

  val decodeLayer: ULayer[Has[Decoder]] =
    (configLayer ++ eventTimeLayer) >>> DecoderLive.layer

  val summaryEmitterLayer: ULayer[Has[SummaryEmitter]] =
    (configLayer ++ eventTimeLayer) >>> SummaryEmitterLive.layer

  val windowSummarizer: ULayer[Has[WindowSummarizer]] =
    (Clock.live ++ eventTimeLayer ++ summaryEmitterLayer) >>> WindowSummarizerLive.layer

  type CustomLayer = Has[TweetStream] with Has[Decoder] with Has[WindowSummarizer] with Has[SummaryEmitter] with Has[EventTime]
  val customLayer = tweetStreamLayer ++ decodeLayer ++ windowSummarizer ++ summaryEmitterLayer ++ eventTimeLayer


  val tweetStream: ZStream[Has[TweetStream], Nothing, Byte] =
    ZStream.unwrap {
      for {
        stream <- TweetStream.tweetStream
      } yield handleFailure(stream)
    }

  /** Handle failures in the tweet stream.
   *
   * The http4s stream can fail with a Throwable.
   * In a real application, we would have a more meaningful way to recover the stream
   * (e.g., reopen it). This should also be aligned with managing the HTTP client.
   * For now, just print the error and terminate the stream.
   */
  def handleFailure(tweetStream: ZStream[Any, Throwable, Byte]): ZStream[Any, Nothing, Byte] =
    tweetStream.catchAll {
      e =>
        println(e) // TODO: Log error
        ZStream()
    }

  val tweetSummaryProgram1: ZIO[Has[Config] with Has[Decoder], Nothing, ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput]] =
    for {
      streamParameters <- Config.streamParameters
      decodeStringToExtract <- DecodeTransducer.decodeStringToExtract
    } yield tweetStream
      .transduce(utf8Decode >>> splitLines)
      .transduce(decodeStringToExtract)

      .groupedWithin(
        chunkSize = streamParameters.chunkSizeLimit,
        within = streamParameters.chunkGroupTimeout)
      .flattenChunks

      .transduce(summarizeChunks)

      .mapM(SummaryEmitter.emitSummary)

  val tweetSummaryProgram2: URIO[Has[Config] with Has[Decoder], ZStream[CustomLayer with Clock, Nothing, WindowSummaryOutput]] =
    for {
      streamParameters <- Config.streamParameters
      decodeStringToExtract <- DecodeTransducer.decodeStringToExtract
    } yield tweetStream
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
