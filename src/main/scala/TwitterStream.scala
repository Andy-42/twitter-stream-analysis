import andy42.twitter.config.{Config, ConfigLive}
import andy42.twitter.tweet.{TweetStream, TweetStreamLive}
import zio.stream.{ZSink, ZStream}
import zio.{ExitCode, Has, ULayer, URIO, ZEnv}

object TwitterStream extends zio.App {

  // Reading the config could produce a ReadError[Sting], but fail right here if that happens.
  val configLayer: ULayer[Has[Config]] =
    ConfigLive.layer.orDie

  val tweetStreamLayer: ULayer[Has[TweetStream]] =
    configLayer >>> TweetStreamLive.layer

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

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    tweetStream.provideCustomLayer(tweetStreamLayer)
      .run(
        ZSink.fromOutputStream(System.out)
      ).exitCode
}
