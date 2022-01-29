package andy42.twitter

import andy42.twitter.config.Config
import andy42.twitter.eventTime.EventTime
import andy42.twitter.summarize.WindowSummary
import zio._

import java.time.Instant

package object output {

  trait SummaryEmitter {
    def emitSummary(windowSummary: WindowSummary): UIO[WindowSummaryOutput]
  }

  case class SummaryEmitterLive(config: Config, eventTime: EventTime) extends SummaryEmitter {

    override def emitSummary(windowSummary: WindowSummary): UIO[WindowSummaryOutput] =
      for {
        summaryOutput <- config.summaryOutput
        topN = summaryOutput.topN
        toWindowEnd <- eventTime.toWindowEnd
      } yield WindowSummaryOutput(
        windowStart = Instant.ofEpochMilli(windowSummary.windowStart).toString,
        windowEnd = Instant.ofEpochMilli(toWindowEnd(windowSummary.windowStart)).toString,
        windowLastUpdate = Instant.ofEpochMilli(windowSummary.lastWindowUpdate).toString,

        tweetCountThisWindow = windowSummary.tweets,

        topEmojis = top(topN, windowSummary.emojiCounts).toList,
        topDomains = top(topN, windowSummary.domainCounts).toList,
        topHashtags = top(topN, windowSummary.hashtagCounts).toList,

        tweetsWithEmojiPercent = 100.0 * windowSummary.tweetsWithEmoji / windowSummary.tweets,
        tweetsWithUrlPercent = 100.0 * windowSummary.tweetsWithUrl / windowSummary.tweets,
        tweetsWithPhotoUrlPercent = 100.0 * windowSummary.tweetsWithPhotoUrl / windowSummary.tweets
      )

    /** Get the top N values by counts in descending order. */
    def top(topN: Int, counts: Map[String, Count]): Seq[String] =
      counts.toSeq
        .sortBy { case (_, count) => -count } // descending
        .take(topN)
        .map { case (key, _) => key } // Keep the keys only
  }

  object SummaryEmitterLive {
    val layer: URLayer[Has[Config] with Has[EventTime], Has[SummaryEmitter]] =
      (SummaryEmitterLive(_, _)).toLayer
  }

  object SummaryEmitter {
    def emitSummary(windowSummary: WindowSummary): URIO[Has[SummaryEmitter], WindowSummaryOutput] =
      ZIO.serviceWith[SummaryEmitter](_.emitSummary(windowSummary))
  }
}
