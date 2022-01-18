package andy42.twitter.output

import andy42.twitter.{Count, Percent}

/** The summary of tweets in a time window.
 *
 * @param windowStart               The window start as an ISO8601 string.
 * @param windowEnd                 The window end as an ISO8601 string.
 * @param windowLastUpdate          The instant when the last update to this window was applied.
 * @param tweetCountThisWindow      The total number of tweets that were counted in this window.
 * @param topEmojis                 The top N emojis occurring within this window.
 * @param topHashtags               The top N hashtags occurring within this window.
 * @param topDomains                The top N domains occurring within this windows.
 * @param tweetsWithEmojiPercent    The percentage of tweets in this window that contain at least one emoji.
 * @param tweetsWithUrlPercent      The percentage of tweets in this window that contain at least one URL.
 * @param tweetsWithPhotoUrlPercent The percentage of tweets in this window that contain at least one photo URL.
 */
case class WindowSummaryOutput(windowStart: String,
                               windowEnd: String,
                               windowLastUpdate: String,

                               tweetCountThisWindow: Count,

                               topEmojis: List[String],
                               topHashtags: List[String],
                               topDomains: List[String],

                               tweetsWithEmojiPercent: Percent,
                               tweetsWithUrlPercent: Percent,
                               tweetsWithPhotoUrlPercent: Percent
                              )