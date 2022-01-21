package andy42.twitter

import andy42.twitter.config.Config
import andy42.twitter.config.Config.TwitterStreamConfig
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.oauth1
import org.http4s.{Method, Request}
import zio._
import zio.interop.catz._
import zio.stream.Stream
import zio.stream.interop.fs2z._

/**
 * Produces an Stream[Byte] containing tweets.
 * Each chunk will contain a sequence of tweets (i.e., not enclosed in a JSON array).
 * The JSON is not parsed at this point.
 *
 * This implementation uses http4s to create the stream, which is then converted to a ZStream.
 * If we were to use zio-http, we would need an implementation of OAuth signing.
 * The Cats implementation is not visible from outside this layer implementation.
 *
 * TODO:
 *  - Implement OAuth request signing and move to a purely ZIO implementation (zio-http).
 *  - Manage the Blaze Client
 *  - A spurious javax.net.ssl.SSLException("closing inbound before receiving peer's close_notify")
 *    on disconnect will be logged (at debug level). This is benign, and is only log clutter.
 *  - Upgrade to Twitter API 2.x
 */
package object tweet {

  trait TweetStream {
    def tweetStream: ZIO[Any, Nothing, Stream[Throwable, Byte]]
  }

  case class TweetStreamLive(config: Config.Service) extends TweetStream {

    val twitterStreamConfig: TwitterStreamConfig = config.twitterStream

    // Provide cats implementations to http4s/fs2
    implicit val runtime: zio.Runtime[ZEnv] = zio.Runtime.default

    val request: Request[Task] = Request[Task](Method.GET, twitterStreamConfig.sampleApiUrl)
    val signRequest: Task[Request[Task]] = sign(twitterStreamConfig)(request)

    override def tweetStream: ZIO[Any, Nothing, Stream[Throwable, Byte]] =
      ZIO.succeed {
        val fs2Stream = for {
          client <- BlazeClientBuilder[Task](runtime.platform.executor.asEC).stream
          signedRequest <- fs2.Stream.eval(signRequest)
          response <- client.stream(signedRequest)
          tweetChunk <- response.body
        } yield tweetChunk

        fs2Stream.toZStream(queueSize = 8196) // TODO: Config
      }

    def sign(config: TwitterStreamConfig)(request: Request[Task]): Task[Request[Task]] = {
      val consumer = oauth1.Consumer(config.apiKey, config.apiKeySecret)
      val token = oauth1.Token(config.accessToken, config.accessTokenSecret)
      oauth1.signRequest(
        req = request, consumer = consumer, callback = None, verifier = None, token = Some(token))
    }
  }

  object TweetStreamLive {
    val layer: URLayer[Has[Config.Service], Has[TweetStream]] =
      (TweetStreamLive(_)).toLayer
  }

  object TweetStream {
    def tweetStream: ZIO[Has[TweetStream], Nothing, Stream[Throwable, Byte]] =
      ZIO.serviceWith[TweetStream](_.tweetStream)
  }
}
