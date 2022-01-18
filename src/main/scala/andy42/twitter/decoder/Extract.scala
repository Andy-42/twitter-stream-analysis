package andy42.twitter.decoder

import andy42.twitter.WindowStart

/** Data extracted from a tweet.
 *
 * @param windowStart The `created_at` timestamp from the tweet, in milliseconds, aligned to the
 *                    start of a summary window. The resolution of a tweet is one second.
 * @param hashTags    The hashtags extracted from the tweet text. The hashtag casing is exactly as it
 *                    it represented in the tweet (i.e., no transformation of the casing).
 *                    A hashtag can occur multiple times in one tweet.
 * @param emojis      The emoji extracted from a tweet text. An emoji can occur multiple times in one tweet.
 *                    An emoji can consist of multiple code points.
 * @param urlDomains  The domains (the host part of HTTP URL references) extracted from one tweet.
 *                    A domain can occur multiple times within one tweet.
 */
case class Extract(windowStart: WindowStart,
                   hashTags: Vector[String],
                   emojis: Vector[String],
                   urlDomains: Vector[String],
                   containsPhotoUrl: Boolean) {

  def containsEmoji: Boolean = emojis.nonEmpty

  def containsUrl: Boolean = urlDomains.nonEmpty
}