package channel

import scala.collection.immutable.Queue


class ReliableChannel extends Channel {
  var messages : Queue[SentMessage] = Queue.empty
  override def send(sender: Channel, msg: Message): Unit = {
    messages = messages.enqueue(SentMessage(sender, msg))
  }

  override def receive() = {
    val h = messages.headOption
    h match {
      case None => Option.empty
      case _ => {
        messages = messages.tail
        h
      }
    }
  }
}

