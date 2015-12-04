package version3

import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import com.datastax.driver.core.TableMetadata

/**
  * Created by gk on 2015/12/3.
  */
class IMLOSink extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = new WatermarkRequestStrategy(12)

  override def receive: Receive = {
    case OnNext(json) =>
      println(json + "xx")
      val m = new TableMetadata

  }
}
