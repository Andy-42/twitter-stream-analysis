package andy42.twitter.summarize

import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import andy42.twitter.eventTime.EventTime.isExpired
import andy42.twitter.summarize.WindowSummary.updateSummaries
import zio.clock.{Clock, currentTime}
import zio.stream.ZTransducer
import zio.{Chunk, Has, ZRef}

import java.util.concurrent.TimeUnit

object SummarizeWindowTransducer {

  val summarizeChunks: ZTransducer[Clock with Has[EventTime], Nothing, Extract, WindowSummary] =
    ZTransducer {
      ZRef.makeManaged[WindowSummaries](EmptyWindowSummaries).map { stateRef =>
        (maybeChunk: Option[Chunk[Extract]]) =>
          for {
            now <- currentTime(TimeUnit.MILLISECONDS)
            isExpired <- isExpired(now)
            nextOutputChunk <- stateRef.modify { windowSummaries =>

              val (expired, ongoing) = windowSummaries.partition {
                case (windowStart, _) => isExpired(windowStart)
              }

              maybeChunk match {

                case None =>
                  Chunk.fromIterable(expired.values) -> ongoing

                case Some(chunk) =>
                  Chunk.fromIterable(expired.values) -> updateSummaries(
                    windowSummaries = ongoing,
                    tweetExtracts = chunk.filter(extract => !isExpired(extract.windowStart)),
                    now = now)
              }
            }
          } yield nextOutputChunk
      }
    }
}
