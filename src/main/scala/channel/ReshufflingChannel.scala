package channel

import scala.util.Random


class ReshufflingChannel(subchannel:Channel) extends Channel {
  var messages:List[(Channel, Message)] = List.empty[(Channel, Message)]

  override def send(sender: Channel, msg: Message): Unit = {
    Console.println(s"sending ${msg}")
    messages = (sender, msg)::messages
    messages = Random.shuffle(messages)
  }

  override def receive(): Option[SentMessage] = {
    if (!messages.isEmpty) {
      val h = messages.head
      Console.println(s"received ${h._2}")
      messages = messages.tail
      subchannel.send(h._1, h._2)
    }
    subchannel.receive()
  }
}
