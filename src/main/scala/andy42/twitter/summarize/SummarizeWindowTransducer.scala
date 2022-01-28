package andy42.twitter.summarize

import andy42.twitter.EpochMillis
import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import andy42.twitter.eventTime.EventTime.isExpiredX
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
            isExpired <- isExpiredX(now)
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

  /** Update the window summaries for each distinct window start time, but only for non-expired windows. */
  def updateSummaries(windowSummaries: WindowSummaries,
                      tweetExtracts: Chunk[Extract],
                      now: EpochMillis): WindowSummaries = {
    val updatedOrNewSummaries = for {
      windowStart <- tweetExtracts.iterator.map(_.windowStart).distinct

      previousSummaryForWindow = windowSummaries.getOrElse(
        key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))

    } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now)

    windowSummaries ++ updatedOrNewSummaries
  }
}
