package andy42.twitter

import andy42.twitter.config.Config
import zio._

package object eventTime {

  trait EventTime {
    /** Move an instant (in millis) to the start of a window */
    def toWindowStart(createdAt: EpochMillis): WindowStart

    def toWindowEnd(createdAt: EpochMillis): EpochMillis

    /** Does an instant (in millis) fall into a fully-expired window?
     * We compare the instant that the window ends to the watermark position (relative to now):
     * if the end of the window is before the watermark, that window is fully expired.
     */
    def isExpired(createdAt: EpochMillis, now: EpochMillis): Boolean

    def isExpiredX(now: EpochMillis): ZIO[Any, Nothing, EpochMillis => Boolean]
  }

  case class EventTimeLive(config: Config) extends EventTime {
    val windowSize = config.configTopLevel.eventTime.windowSize.toMillis
    val watermark = config.configTopLevel.eventTime.watermark.toMillis

    /** Move an instant (in millis) to the start of a window */
    override def toWindowStart(createdAt: EpochMillis): WindowStart =
      createdAt - (createdAt % windowSize)

    override def toWindowEnd(createdAt: EpochMillis): EpochMillis =
      toWindowStart(createdAt) + windowSize - 1

    /** Does an instant (in millis) fall into a fully-expired window?
     * We compare the instant that the window ends to the watermark position (relative to now):
     * if the end of the window is before the watermark, that window is fully expired.
     */
    override def isExpired(createdAt: EpochMillis, now: EpochMillis): Boolean =
      toWindowEnd(createdAt) < (now - watermark)

    override def isExpiredX(now: EpochMillis): ZIO[Any, Nothing, EpochMillis => Boolean] =
      ZIO.succeed((createdAt: EpochMillis) => isExpired(createdAt, now))
  }

  object EventTimeLive {
    val layer: URLayer[Has[Config], Has[EventTime]] = (EventTimeLive(_)).toLayer
  }

  object EventTime {
    // TODO: Rename
    def isExpiredX(createdAt: EpochMillis): ZIO[Has[EventTime], Nothing, EpochMillis => Boolean] =
      ZIO.serviceWith[EventTime](_.isExpiredX(createdAt))
  }
}
