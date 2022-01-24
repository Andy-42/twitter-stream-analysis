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
  }

  case class EventTimeLive(config: Config) extends EventTime {
    val eventTimeConfig = config.configTopLevel.eventTime

    /** Move an instant (in millis) to the start of a window */
    def toWindowStart(createdAt: EpochMillis): WindowStart =
      createdAt - (createdAt % eventTimeConfig.windowSize.toMillis)

    def toWindowEnd(createdAt: EpochMillis): EpochMillis =
      toWindowStart(createdAt) + eventTimeConfig.windowSize.toMillis - 1

    /** Does an instant (in millis) fall into a fully-expired window?
     * We compare the instant that the window ends to the watermark position (relative to now):
     * if the end of the window is before the watermark, that window is fully expired.
     */
    def isExpired(createdAt: EpochMillis, now: EpochMillis): Boolean =
      toWindowEnd(createdAt) < (now - eventTimeConfig.watermark.toMillis)
  }

  object EventTimeLive {
    val layer: URLayer[Has[Config], Has[EventTime]] = (EventTimeLive(_)).toLayer
  }

  // No accessor for EventTime since it is not used at the business logic level
}
