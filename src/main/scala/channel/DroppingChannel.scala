package channel

class DroppingChannel(subchannel:Channel, var dropPattern:Stream[Boolean]) extends Channel {
  val duplicatingChannel = new DuplicatingChannel(subchannel, dropPattern.map(b => if (b) 1 else 0))
  override def send(sender: Channel, msg: Message): Unit = duplicatingChannel.send(sender, msg)
  override def receive() = duplicatingChannel.receive()
}
