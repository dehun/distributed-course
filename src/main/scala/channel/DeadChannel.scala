package channel

class DeadChannel extends Channel{
  override def send(sender: Channel, msg: Message): Unit = {}
  override def receive(): Option[SentMessage] = None
}
