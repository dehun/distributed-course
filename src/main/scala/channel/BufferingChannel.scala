package channel

class BufferingChannel(subchannel: Channel, buffering: Int, delay: Int) extends Channel {
  var messages = List.empty[(Int, Channel, Message)]

  override def send(sender: Channel, msg: Message): Unit = messages = (0, sender, msg)::messages

  override def receive(): Option[SentMessage] = {
    if (messages.size > buffering) {
      val (l, r) = messages.splitAt(buffering)
      messages = r
      l.foreach(m => subchannel.send(m._2, m._3))
    }
    messages = messages.map({ case (rs, sender, msg) => (rs + 1, sender, msg) })
    val (l, r) = messages.partition(_._1 > delay)
    messages = r
    l.foreach(m => subchannel.send(m._2, m._3))

    subchannel.receive()
  }
}
