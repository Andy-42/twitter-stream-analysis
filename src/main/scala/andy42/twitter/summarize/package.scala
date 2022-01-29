package andy42.twitter

import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import zio._
import zio.clock.Clock
import zio.stream.{UStream, ZStream}

import java.util.concurrent.TimeUnit.MILLISECONDS

package object summarize {

  type WindowSummaries = Map[WindowStart, WindowSummary]
  val EmptyWindowSummaries: WindowSummaries = Map.empty

  trait WindowSummarizer {
    def addChunkToSummary(summariesByWindow: WindowSummaries,
                          chunk: Chunk[Extract]
                         ): UIO[(WindowSummaries, UStream[WindowSummary])]
  }

  case class WindowSummarizerLive(clock: Clock.Service,
                                  eventTime: EventTime) extends WindowSummarizer {

    override def addChunkToSummary(summariesByWindow: WindowSummaries,
                                   tweetExtracts: Chunk[Extract]
                                  ): UIO[(WindowSummaries, UStream[WindowSummary])] =
      for {
        now <- clock.currentTime(MILLISECONDS)
        isExpired <- eventTime.isExpired(now)

        (expired, ongoing) = summariesByWindow.partition {
          case (windowStart, _) => isExpired(windowStart)
        }

        updatedSummaries = updateSummaries(
          summariesByWindow = ongoing,
          tweetExtracts = tweetExtracts.filter(extract => !isExpired(extract.windowStart)),
          now = now)

      } yield (updatedSummaries, ZStream.fromIterable(expired.values))

    // TODO: This code is shared with SummarizeWindowTransducer.

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
