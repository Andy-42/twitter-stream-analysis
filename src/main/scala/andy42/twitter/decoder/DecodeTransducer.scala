package andy42.twitter.decoder

import andy42.twitter.decoder.CreateTweet.isCreateTweet
import zio.stream.ZTransducer
import zio.{Chunk, Has, ZIO}

object DecodeTransducer {

  val decodeStringToExtract: ZIO[Has[Decoder], Nothing, ZTransducer[Any, Nothing, String, Extract]] =
    for {
      decodeLineToExtract <- Decoder.decodeLineToExtract
    } yield ZTransducer
      .fromFunction(decodeLineToExtract)
      .filterInput(isCreateTweet) >>>
      ZTransducer.fromPush[Any, Nothing, Either[String, Extract], Extract] {
        case None => ZIO.succeed(Chunk.empty)
        case Some(chunk) => ZIO.succeed {
          val (decodeFailure, decodeSuccess) = chunk.partitionMap(identity)
          // TODO: Log decode failures
          decodeSuccess
        }
      }
}
