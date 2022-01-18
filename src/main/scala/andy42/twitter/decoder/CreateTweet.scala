package andy42.twitter.decoder

object CreateTweet {

  // The prefix of a create tweet, which we can use to filter the ones we are interested in.
  private val Prefix = "{\"created_at\":"

  def isCreateTweet(tweetText: String): Boolean = tweetText.startsWith(Prefix)
}
