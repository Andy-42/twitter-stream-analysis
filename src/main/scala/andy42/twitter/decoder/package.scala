package andy42.twitter

import andy42.twitter.config.Config
import andy42.twitter.eventTime.EventTime
import com.twitter.twittertext.{Extractor, TwitterTextEmojiRegex}
import io.circe.HCursor
import io.circe.parser._
import zio.{Has, IO, ZIO, ZLayer}

import java.net.URL
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

package object decoder {

  // TODO: Collect all errors (not just first)

  type Decoder = Has[Decoder.Service]

  object Decoder {

    trait Service {
      def decodeLineToExtract(line: String): IO[String, Extract]
    }

    val live: ZLayer[Has[Config.Service] with EventTime, Throwable, Decoder] =
      ZLayer.fromServices[Config.Service, EventTime.Service, Decoder.Service] {
        (config, eventTime) => {
//          implicit val runtime: URIO[ZEnv, zio.Runtime[ZEnv]] = ZIO.runtime[ZEnv]
          val photoDomains = config.summaryOutput.photoDomains

          new Service {

            override def decodeLineToExtract(line: String): IO[String, Extract] = {

              // TODO: Capture full parse failure detail, log failures

              // TODO: If "created_at" doesn't exist, don't attempt to parse
              // TODO: If parsing fails for some reason, log it and filter out - use transducer instead since this is not just parsing

              for {
                cursor <- ZIO.fromEither(parse(line)).mapBoth(
                  parsingFailure => parsingFailure.message,
                  json => json.hcursor
                )
                createdAt <- getStringField(cursor, "created_at")
                text <- getStringField(cursor, "text")
                parsedDate <- parseDate(createdAt)
                urlDomains <- parseUrlDomains(extractUrls(text)) // Fails if parsing any URL fails
              } yield Extract(
                windowStart = eventTime.toWindowStart(parsedDate),
                hashTags = extractHashTags(text),
                emojis = extractEmojis(text),
                urlDomains = urlDomains,
                containsPhotoUrl = urlDomains.exists(photoDomains.contains)
              )
            }
          }
        }
      }

    def decodeLineToExtract(line: String): ZIO[Decoder, String, Extract] = ZIO.accessM(_.get.decodeLineToExtract(line))

    private def getStringField(hCursor: HCursor, name: String): IO[String, String] =
      hCursor.get[String](name) match {
        case Right(s) => ZIO.succeed(s)
        case Left(decodingFailure) => ZIO.fail(error = s"get $name - ${decodingFailure.message}")
      }

    // For decoding tweet timestamps.
    private[this] val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

    def parseDate(dateString: String): IO[String, EpochMillis] =
      Try(Instant.from(formatter.parse(dateString)).toEpochMilli)
        .fold(
          throwable => ZIO.fail(error = s"parseDate - ${throwable.getMessage}"),
          epochMillis => ZIO.succeed(epochMillis))

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

    def parseUrlDomains(urlStrings: Vector[String]): IO[String, Vector[String]] =
      urlStrings.map(urlString => Try(new URL(urlString))) match {
        case maybeURLs if maybeURLs.exists(_.isFailure) =>
          ZIO.fail("parseUrl") // TODO: Error detail
        case urls =>
          ZIO.succeed(urls.map(_.get.getHost))
      }
  }
}
