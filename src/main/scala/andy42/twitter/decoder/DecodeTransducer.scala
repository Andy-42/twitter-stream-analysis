package andy42.twitter.decoder

import andy42.twitter.decoder.CreateTweet.isCreateTweet
import zio.stream.{Transducer, ZTransducer}
import zio.{Chunk, Has, URIO, ZIO}

object DecodeTransducer {

  val decodeStringToExtract: URIO[Has[Decoder], Transducer[Nothing, String, Extract]] =
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
