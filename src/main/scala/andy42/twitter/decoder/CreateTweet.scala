package andy42.twitter.decoder

/**
 * A class the represents the fields of a tweet that we are interested in.
 * @param created_at
 * @param text
 */
case class CreateTweet(created_at: String, text: String)


object CreateTweet {

  // The prefix of a create tweet, which we can use to filter the ones we are interested in.
  private val Prefix = "{\"created_at\":"

  def isCreateTweet(tweetText: String): Boolean = tweetText.startsWith(Prefix)
}
