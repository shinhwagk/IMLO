package org.gk.imlo.source

import akka.actor._

class SourceSlaveOracle extends Actor with ActorLogging {

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
    self.tell(message.get.asInstanceOf[Long],sender())
  }

  override def receive: Receive = {
    case tid: Long =>
//      sender() ! SourceActuator.getRows(tid)
  }
}