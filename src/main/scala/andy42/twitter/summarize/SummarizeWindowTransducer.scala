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
        (inputMaybeChunk: Option[Chunk[Extract]]) =>
          for {
            now <- currentTime(TimeUnit.MILLISECONDS)
            isExpired <- isExpiredX(now)
            nextOutputChunk <- stateRef.modify { windowSummaries =>
              inputMaybeChunk match {

                case None =>
                  if (windowSummaries.isEmpty) {
                    Chunk.empty -> EmptyWindowSummaries
                  } else {
                    val (expired, ongoing) = partitionExpiredAndOngoing(windowSummaries, isExpired)

                    Chunk.fromIterable(expired.values) -> ongoing
                  }

                case Some(chunk: Chunk[Extract]) =>
                  val (expired, ongoing) = partitionExpiredAndOngoing(windowSummaries, isExpired)

                  val updatedSummaries = updateSummaries(
                    summariesByWindow = ongoing,
                    chunk.filter(extract => !isExpired(extract.windowStart)), // Don't update expired windows
                    now = now)

                  Chunk.fromIterable(expired.values) -> updatedSummaries
              }
            }
          } yield nextOutputChunk
      }
    }

  def partitionExpiredAndOngoing(summariesByWindow: WindowSummaries,
                                 isExpired: EpochMillis => Boolean
                                ): (WindowSummaries, WindowSummaries) =
    summariesByWindow.partition {
      case (windowStart, _) => isExpired(windowStart)
    }

  /** Update the window summaries for each distinct window start time, but only for non-expired windows. */
  def updateSummaries(summariesByWindow: WindowSummaries,
                      tweetExtracts: Chunk[Extract],
                      now: EpochMillis): WindowSummaries = {
    val updatedOrNewSummaries = for {
      windowStart <- tweetExtracts.iterator.map(_.windowStart).distinct

      previousSummaryForWindow = summariesByWindow.getOrElse(
        key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))

    } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now)

    summariesByWindow ++ updatedOrNewSummaries
  }
}
