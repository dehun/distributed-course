package channel

class DuplicatingChannel(subchannel:Channel, var sendPattern:Stream[Int]) extends Channel {
  override def send(sender: Channel, msg: Message): Unit = {
    val h = sendPattern.head
    sendPattern = sendPattern.tail
    (1 to h).foreach(_ => subchannel.send(sender, msg))
  }

  override def receive() = subchannel.receive()
}
