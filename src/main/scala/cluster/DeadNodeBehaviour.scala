package cluster
import channel.{Channel, Message}
import storage.Storage

class DeadNodeBehaviour extends NodeBehaviour {
  override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = { }
  override def tick(time: Int, node: Node): Unit = { }
}
