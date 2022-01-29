package andy42.twitter

import andy42.twitter.config.Config
import zio._

package object eventTime {

  trait EventTime {
    /** Move an instant (in millis) to the start of a window */
    def toWindowStart: UIO[EpochMillis => WindowStart]

    def toWindowEnd: UIO[EpochMillis => WindowEnd]

    /** Does an instant (in millis) fall into a fully-expired window?
     * We compare the instant that the window ends to the watermark position (relative to now):
     * if the end of the window is before the watermark, that window is fully expired.
     */
    def isExpired(now: EpochMillis): UIO[EpochMillis => Boolean]
  }

  case class EventTimeLive(config: Config) extends EventTime {

    /** Move an instant (in millis) to the start of a window */
    override def toWindowStart: UIO[EpochMillis => WindowStart] =
      for {
        eventTime <- config.eventTime
        windowSize = eventTime.windowSize.toMillis
      } yield (instant: EpochMillis) => instant - (instant % windowSize)

    override def toWindowEnd: UIO[EpochMillis => WindowEnd] =
      for {
        eventTime <- config.eventTime
        windowSize = eventTime.windowSize.toMillis
        toWindowStart <- toWindowStart
      } yield (instant: EpochMillis) => toWindowStart(instant) + windowSize - 1

    /** Does an instant (in millis) fall into a fully-expired window?
     * We compare the instant that the window ends to the watermark position (relative to now):
     * if the end of the window is before the watermark, that window is fully expired.
     */
    override def isExpired(now: EpochMillis): UIO[EpochMillis => Boolean] =
      for {
        eventTime <- config.eventTime
        watermark = eventTime.watermark.toMillis
        toWindowEnd <- toWindowEnd
      } yield (instant: EpochMillis) => toWindowEnd(instant) < (now - watermark)
  }

  object EventTimeLive {
    val layer: URLayer[Has[Config], Has[EventTime]] = (EventTimeLive(_)).toLayer
  }

  object EventTime {
    def toWindowStart: URIO[Has[EventTime], EpochMillis => WindowStart] =
      ZIO.serviceWith[EventTime](_.toWindowStart)

    def toWindowEnd: URIO[Has[EventTime], EpochMillis => WindowStart] =
      ZIO.serviceWith[EventTime](_.toWindowEnd)

    def isExpired(now: EpochMillis): URIO[Has[EventTime], EpochMillis => Boolean] =
      ZIO.serviceWith[EventTime](_.isExpired(now))
  }
}
