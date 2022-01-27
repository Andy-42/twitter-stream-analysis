package andy42.twitter

import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import zio._
import zio.clock.Clock
import zio.clock.currentTime
import zio.stream.{Transducer, UStream, ZStream, ZTransducer}

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

package object summarize {

  type WindowSummaries = Map[WindowStart, WindowSummary]
  val EmptyWindowSummaries: WindowSummaries = Map.empty

  trait WindowSummarizer {
    def addChunkToSummary(summariesByWindow: WindowSummaries,
                          chunk: Chunk[Extract]
                         ): UIO[(WindowSummaries, UStream[WindowSummary])]

    def summarizeChunks: ZTransducer[Clock, Nothing, Extract, WindowSummary]
  }

  case class WindowSummarizerLive(clock: Clock.Service,
                                  eventTime: EventTime) extends WindowSummarizer {

    override def addChunkToSummary(summariesByWindow: WindowSummaries,
                                   tweetExtracts: Chunk[Extract]
                                  ): UIO[(WindowSummaries, UStream[WindowSummary])] =
      for {
        now <- clock.currentTime(MILLISECONDS)

        updatedSummaries = updateSummaries(summariesByWindow, tweetExtracts, now)

        // Separate expired windows from windows for which collection is ongoing
        (expired, ongoing) = updatedSummaries.partition { case (windowStart, _) =>
          eventTime.isExpired(createdAt = windowStart, now = now)
        }

        // Any summaries that are expired get reported
        //output = expired.values.map(summaryEmitter.emitSummary)

      } yield (ongoing, ZStream.fromIterable(expired.values))

    /** Update the window summaries for each distinct window start time, but only for non-expired windows. */
    def updateSummaries(summariesByWindow: WindowSummaries,
                        tweetExtracts: Chunk[Extract],
                        now: EpochMillis): WindowSummaries = {
      val updatedOrNewSummaries = for {
        windowStart <- tweetExtracts.iterator.map(_.windowStart).distinct
        if !eventTime.isExpired(createdAt = windowStart, now = now)

        previousSummaryForWindow = summariesByWindow.getOrElse(
          key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))

      } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now)

      summariesByWindow ++ updatedOrNewSummaries
    }

    override def summarizeChunks: ZTransducer[Clock, Nothing, Extract, WindowSummary] =
      ZTransducer {
        for {
          stateRef <- ZRef.makeManaged[WindowSummaries](EmptyWindowSummaries)
          now <- ZManaged.fromEffect(currentTime(TimeUnit.MILLISECONDS)) // FIXME: Is this right?
        } yield {
          case None =>

            stateRef.modify { windowSummaries =>
              if (windowSummaries.isEmpty) {
                Chunk.empty -> EmptyWindowSummaries
              } else {
                val (expired, ongoing) = partitionExpiredAndOngoing(windowSummaries, now)
                Chunk.fromIterable(expired.values) -> ongoing
              }
            }

          case Some(chunk: Chunk[Extract]) =>
            stateRef.modify { windowSummaries =>
              val (expired, ongoing) = partitionExpiredAndOngoing(windowSummaries, now)
              val updatedSummaries = updateSummaries(
                summariesByWindow = ongoing,
                chunk.filter(extract => !eventTime.isExpired(createdAt = extract.windowStart, now = now)),
                now = now)

              Chunk.fromIterable(expired.values) -> updatedSummaries
            }
        }
      }

    def partitionExpiredAndOngoing(summariesByWindow: WindowSummaries,
                                   now: EpochMillis
                                  ): (WindowSummaries, WindowSummaries) =
      summariesByWindow.partition {
        case (windowStart, _) =>
          eventTime.isExpired(createdAt = windowStart, now = now)
      }
  }

  object WindowSummarizerLive {
    val layer: URLayer[Clock with Has[EventTime], Has[WindowSummarizer]] =
      (WindowSummarizerLive(_, _)).toLayer
  }

  object WindowSummarizer {
    def addChunkToSummary(summariesByWindow: WindowSummaries,
                          tweetExtracts: Chunk[Extract]
                         ): URIO[Has[WindowSummarizer], (WindowSummaries, UStream[WindowSummary])] =
      ZIO.serviceWith[WindowSummarizer](_.addChunkToSummary(summariesByWindow, tweetExtracts))
  }
}
