package cluster
import channel.{Channel, Message}
import storage.Storage

class EchoBehaviour extends NodeBehaviour {
  override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = {
    sender.send(sender, msg)
  }

  override def tick(time: Int, node: Node): Unit = {

  }
}
