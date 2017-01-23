package channel

import java.util.function.Predicate

class IfChannel(predicate: (Message) => Boolean, thenCh:Channel, elseCh:Channel) extends Channel{
  override def send(sender: Channel, msg: Message): Unit =
    if (predicate(msg)) thenCh.send(sender, msg)
    else elseCh.send(sender, msg)
  override def receive(): Option[SentMessage] = thenCh.receive() orElse elseCh.receive()
}
