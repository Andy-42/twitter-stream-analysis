package andy42.twitter.decoder

import andy42.twitter.decoder.CreateTweet.isCreateTweet
import andy42.twitter.decoder.Decoder.decodeLineToExtract
import zio.{Chunk, ZIO}
import zio.stream.ZTransducer

object DecodeTransducer {

  val decodeStringToExtract: ZTransducer[Decoder, Nothing, String, Extract] =
    ZTransducer
      .fromFunctionM(decodeLineToExtract)
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
