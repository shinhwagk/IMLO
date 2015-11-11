package test.actor.publisher

import akka.actor.{Actor, ActorLogging}
import akka.stream.actor.ActorPublisher
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.source.SourceActuator

/**
  * Created by gk on 2015/11/10.
  */
class PubOracle extends ActorPublisher[RowsInfo] with ActorLogging {

  override def receive: Receive = {
    case null =>
  }
}

class PubOracleSlave extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("���·�����:" + self.path + ". ��:" + sender().path)
    self.tell(message.get.asInstanceOf[Long], sender())
  }

  override def receive: Receive = {
    case tid: Long =>
//      sender() ! SourceActuator.getRows
  }
}