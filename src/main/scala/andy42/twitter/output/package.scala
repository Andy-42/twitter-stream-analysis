package andy42.twitter

import andy42.twitter.config.Config
import andy42.twitter.eventTime.EventTime
import andy42.twitter.summarize.WindowSummary
import zio.{Has, ZIO, ZLayer}

import java.time.Instant

package object output {

  type SummaryEmitter = Has[SummaryEmitter.Service]

  object SummaryEmitter {

    trait Service {
      def emitSummary(windowSummary: WindowSummary): WindowSummaryOutput
    }

    val live: ZLayer[Has[Config.Service] with EventTime, Throwable, SummaryEmitter] =
      ZLayer.fromServices[Config.Service, EventTime.Service, SummaryEmitter.Service] {
        (config, eventTime) => {

          val topN = config.summaryOutput.topN
          new Service {

            override def emitSummary(windowSummary: WindowSummary): WindowSummaryOutput =
              WindowSummaryOutput(
                windowStart = Instant.ofEpochMilli(windowSummary.windowStart).toString,
                windowEnd = Instant.ofEpochMilli(eventTime.toWindowEnd(windowSummary.windowStart)).toString,
                windowLastUpdate = Instant.ofEpochMilli(windowSummary.lastWindowUpdate).toString,

                tweetCountThisWindow = windowSummary.tweets,

                topEmojis = top(topN, windowSummary.emojiCounts).toList,
                topDomains = top(topN, windowSummary.domainCounts).toList,
                topHashtags = top(topN, windowSummary.hashtagCounts).toList,

                tweetsWithEmojiPercent = 100.0 * windowSummary.tweetsWithEmoji / windowSummary.tweets,
                tweetsWithUrlPercent = 100.0 * windowSummary.tweetsWithUrl / windowSummary.tweets,
                tweetsWithPhotoUrlPercent = 100.0 * windowSummary.tweetsWithPhotoUrl / windowSummary.tweets
              )
          }
        }
      }

    def emitSummary(windowSummary: WindowSummary): ZIO[SummaryEmitter, Nothing, WindowSummaryOutput] =
      ZIO.access(_.get.emitSummary(windowSummary))
  }

  /** Get the top N values by counts in descending order. */
  def top(topN: Int, counts: Map[String, Count]): Seq[String] =
    counts.toSeq
      .sortBy { case (_, count) => -count } // descending
      .take(topN)
      .map { case (key, _) => key } // Keep the keys only
}
