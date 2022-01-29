package andy42.twitter

import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import andy42.twitter.summarize.WindowSummary.updateSummaries
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

    override def addChunkToSummary(windowSummaries: WindowSummaries,
                                   tweetExtracts: Chunk[Extract]
                                  ): UIO[(WindowSummaries, UStream[WindowSummary])] =
      for {
        now <- clock.currentTime(MILLISECONDS)
        isExpired <- eventTime.isExpired(now)

        (expired, ongoing) = windowSummaries.partition {
          case (windowStart, _) => isExpired(windowStart)
        }

        updatedSummaries = updateSummaries(
          windowSummaries = ongoing,
          tweetExtracts = tweetExtracts.filter(extract => !isExpired(extract.windowStart)),
          now = now)

      } yield (updatedSummaries, ZStream.fromIterable(expired.values))
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
