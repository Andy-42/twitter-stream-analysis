package andy42.twitter.summarize

import andy42.twitter.decoder.Extract
import andy42.twitter.{Count, EpochMillis, WindowStart}
import zio.Chunk

import scala.annotation.tailrec
import scala.collection.mutable

/** The summary of tweets within a given window.
 *
 * @param windowStart        The time that the tweet was created, adjusted to the start of the window
 *                           that the original created_at falls into.
 * @param lastWindowUpdate   The last time that an update was applied to this window summary.
 * @param tweets             The count of the number of tweets in this window.
 * @param tweetsWithEmoji    The count of tweets in this window that contain at least one emoji in the text.
 * @param tweetsWithUrl      The count of tweets in this window that contain at least one URL in the text.
 * @param tweetsWithPhotoUrl The count of tweets in this window that contain at least one photo URL in the text.
 * @param hashtagCounts      The count of the number of times each hashtag occurs in tweets in this window.
 * @param domainCounts       The count of the number of times each domain occurs in tweets in this window.
 * @param emojiCounts        The count of the number of times each emoji (or emoji sequence) occurs in this window.
 */
case class WindowSummary(windowStart: WindowStart,
                         lastWindowUpdate: EpochMillis,
                         tweets: Count,
                         tweetsWithEmoji: Count,
                         tweetsWithUrl: Count,
                         tweetsWithPhotoUrl: Count,
                         hashtagCounts: Map[String, Count],
                         domainCounts: Map[String, Count],
                         emojiCounts: Map[String, Count]) {

  import WindowSummary.addCounts

  /** Add the occurrences from a Chunk[TweetExtract] to a existing WindowSummary */
  def add(tweetExtracts: Chunk[Extract], now: EpochMillis): WindowSummary = {

    val tweetsInThisWindow = tweetExtracts.filter(_.windowStart == windowStart)

    WindowSummary(
      windowStart = windowStart,
      lastWindowUpdate = now,
      tweets = tweets + tweetExtracts.size,

      tweetsWithEmoji = tweetsWithEmoji + tweetsInThisWindow.count(_.containsEmoji),
      tweetsWithUrl = tweetsWithUrl + tweetsInThisWindow.count(_.containsUrl),
      tweetsWithPhotoUrl = tweetsWithPhotoUrl + tweetsInThisWindow.count(_.containsPhotoUrl),

      hashtagCounts = addCounts(hashtagCounts, tweetsInThisWindow.flatMap(_.hashTags)),
      domainCounts = addCounts(domainCounts, tweetsInThisWindow.flatMap(_.urlDomains)),
      emojiCounts = addCounts(emojiCounts, tweetsInThisWindow.flatMap(_.emojis))
    )
  }
}

object WindowSummary {

  def apply(windowStart: WindowStart, now: EpochMillis) =
    new WindowSummary(
      windowStart = windowStart,
      lastWindowUpdate = now,
      tweets = 0,
      tweetsWithEmoji = 0,
      tweetsWithUrl = 0,
      tweetsWithPhotoUrl = 0,
      hashtagCounts = Map.empty,
      domainCounts = Map.empty,
      emojiCounts = Map.empty
    )

  /** Calculate the count for each occurrence of a String.
   *
   * This method is approximately equivalent to calling:
   * occurrences.toSeq.groupMapReduce(identity)(_ => 1L)(_ + _)
   * ...but saves creating a significant amount of heap noise:
   *  - The Iterator does not need to be forced to an Iterable (i.e., copy Iterator to a List via toSeq
   *    in order to make it work with groupMapReduce). Converting an Iterator -> Iterable and then iterating
   *    exactly once seems wasteful!
   *  - groupMapReduce creates a mutable.Map and then converts it to an immutable.Map before returning.
   *    While we prefer immutable, this mutable state is transient and does not escape the context of
   *    the addCounts calculation. There is absolutely no need to perform this copy.
   *
   * The net effect is the same, but this significantly reduces heap allocation.
   */
  def occurrenceCounts(occurrences: Seq[String]): mutable.Map[String, Count] = {

    @tailrec
    def accumulate(m: mutable.Map[String, Count] = mutable.Map.empty[String, Count],
                   i: Int = 0
                  ): mutable.Map[String, Count] =
      if (i == occurrences.length)
        m
      else {
        val k = occurrences(i)
        m += k -> m.getOrElse(k, 0L)
        accumulate(m = m, i = i + 1)
      }

    accumulate()
  }

  /** Add the count of each occurrence of a key to the counts. */
  def addCounts(counts: Map[String, Count], occurrences: Seq[String]): Map[String, Count] =
    counts ++ occurrenceCounts(occurrences).map { case (occurrence, count) =>
      occurrence -> (count + counts.getOrElse(occurrence, 0L))
    }
}
