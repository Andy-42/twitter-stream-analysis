package andy42.twitter

import andy42.twitter.config.Config
import andy42.twitter.eventTime.EventTime
import com.twitter.twittertext.{Extractor, TwitterTextEmojiRegex}
import io.circe.HCursor
import io.circe.parser._
import zio._

import java.net.URL
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

package object decoder {

  // TODO: Collect all errors (not just first) - maybe use Validated?

  trait Decoder {
    // TODO: Specific failure trait
    def decodeLineToExtract(line: String): UIO[Either[String, Extract]]
  }

  case class DecoderLive(config: Config, eventTime: EventTime) extends Decoder {

    // TODO: Why the wrapping with a ZIO?
    override def decodeLineToExtract(line: String): UIO[Either[String, Extract]] = ZIO.succeed {

      // TODO: Capture full parse failure detail, log failures

      // TODO: If "created_at" doesn't exist, don't attempt to parse
      // TODO: If parsing fails for some reason, log it and filter out - use transducer instead since this is not just parsing

      for {
        cursor <- parse(line) match {
          case Left(parsingFailure) => Left(parsingFailure.message)
          case Right(json) => Right(json.hcursor)
        }
        createdAt <- getStringField(cursor, "created_at")
        text <- getStringField(cursor, "text")
        parsedDate <- parseDate(createdAt)
        urlDomains <- parseUrlDomains(extractUrls(text)) // Fails if parsing any URL fails
      } yield Extract(
        windowStart = eventTime.toWindowStart(parsedDate),
        hashTags = extractHashTags(text),
        emojis = extractEmojis(text),
        urlDomains = urlDomains,
        containsPhotoUrl = urlDomains.exists(config.summaryOutput.photoDomains.contains)
      )
    }

    private def getStringField(hCursor: HCursor, name: String): Either[String, String] =
      hCursor.get[String](name) match {
        case Right(s) => Right(s) // FIXME
        case Left(decodingFailure) => Left(s"get $name - ${decodingFailure}")
      }

    // For decoding tweet timestamps.
    private[this] val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

    def parseDate(dateString: String): Either[String, EpochMillis] =
      Try(Instant.from(formatter.parse(dateString)).toEpochMilli) match {

        case Failure(throwable) => Left(s"parseDate - ${throwable.getMessage}")
        case Success(epochMillis) => Right(epochMillis)
      }

    // Use the twitter text library Extractor to extract hashtags and URLs from text
    private[this] val extractor = new Extractor()

    // Use the twitter text library pattern for extracting emoji from text
    private[this] val emojiRegex = new Regex(TwitterTextEmojiRegex.VALID_EMOJI_PATTERN.pattern)

    def extractHashTags(text: String): Vector[String] = extractor.extractHashtags(text).asScala.toVector

    def extractEmojis(text: String): Vector[String] = emojiRegex.findAllIn(text).toVector

    /** Extract URLs from `text`, and then the domains (host) from the URL text.
     *
     * While we would expect that all URLs will be valid (because they have already been
     * validated by the extraction regular expression), we will fail here if any URL that
     * has been extracted can't be parsed.
     */
    def extractUrls(text: String): Vector[String] = extractor.extractURLs(text).asScala.toVector

    def parseUrlDomains(urlStrings: Vector[String]): Either[String, Vector[String]] =
      urlStrings.map(urlString => Try(new URL(urlString))) match {
        case maybeURLs if maybeURLs.exists(_.isFailure) =>
          Left("parseUrl") // TODO: Error detail
        case urls =>
          Right(urls.map(_.get.getHost))
      }
  }

  object DecoderLive {
    val layer: URLayer[Has[Config] with Has[EventTime], Has[Decoder]] =
      (DecoderLive(_, _)).toLayer
  }

  object Decoder {
    def decodeLineToExtract(line: String): ZIO[Has[Decoder], Nothing, Either[String, Extract]] =
      ZIO.serviceWith[Decoder](_.decodeLineToExtract(line))
  }
}
