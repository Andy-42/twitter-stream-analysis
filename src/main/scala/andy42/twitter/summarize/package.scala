package andy42.twitter

import andy42.twitter.decoder.Extract
import andy42.twitter.eventTime.EventTime
import andy42.twitter.output.{SummaryEmitter, WindowSummaryOutput}
import zio.clock.Clock
import zio.stream.{UStream, ZStream}
import zio.{Chunk, Has, UIO, ZIO, ZLayer}

import java.util.concurrent.TimeUnit.MILLISECONDS

package object summarize {

  type WindowSummarizer = Has[WindowSummarizer.Service]

  type WindowSummaries = Map[WindowStart, WindowSummary]
  val EmptyWindowSummaries: WindowSummaries = Map.empty

  object WindowSummarizer {
    trait Service {
      def addChunkToSummary(summariesByWindow: WindowSummaries,
                            chunk: Chunk[Extract]
                           ): UIO[(WindowSummaries, UStream[WindowSummaryOutput])]
    }

    val live: ZLayer[Clock with EventTime with SummaryEmitter, Nothing, WindowSummarizer] =
      ZLayer.fromServices[Clock.Service, EventTime.Service, SummaryEmitter.Service, WindowSummarizer.Service] {
        (clock, eventTime, summaryEmitter) =>
          new Service {
            override def addChunkToSummary(summariesByWindow: WindowSummaries,
                                           tweetExtracts: Chunk[Extract]
                                          ): UIO[(WindowSummaries, UStream[WindowSummaryOutput])] =
              for {
                now <- clock.currentTime(MILLISECONDS)

                updatedSummaries = updateSummaries(summariesByWindow, tweetExtracts, now, eventTime)

                // Separate expired windows from windows for which collection is ongoing
                (expired, ongoing) = updatedSummaries.partition { case (windowStart, _) =>
                  eventTime.isExpired(createdAt = windowStart, now = now)
                }

                // Any summaries that are expired get reported
                output = expired.values.map(summaryEmitter.emitSummary)

              } yield (ongoing, ZStream(output.toSeq: _*)) // TODO: Better ctor of stream?
          }
      }

    /** Update the window summaries for each distinct window start time, but only for non-expired windows. */
    def updateSummaries(summariesByWindow: WindowSummaries,
                        tweetExtracts: Chunk[Extract],
                        now: Long,
                        eventTime: EventTime.Service): WindowSummaries = {
      val updatedOrNewSummaries = for {
        windowStart <- tweetExtracts.iterator.map(_.windowStart).distinct
        if !eventTime.isExpired(createdAt = windowStart, now = now)

        previousSummaryForWindow = summariesByWindow.getOrElse(
          key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))

      } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now)

      summariesByWindow ++ updatedOrNewSummaries
    }


    def addChunkToSummary(summariesByWindow: WindowSummaries,
                          chunk: Chunk[Extract]
                         ): ZIO[WindowSummarizer, Nothing, (WindowSummaries, UStream[WindowSummaryOutput])] =
      ZIO.accessM(_.get.addChunkToSummary(summariesByWindow, chunk))
  }
}
