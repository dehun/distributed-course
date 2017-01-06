package cluster
import channel.{Channel, Message}
import storage.Storage

case class IntMessage(value:Int) extends Message

class AcceptEverythingBehaviour extends NodeBehaviour {
  override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = {
    msg match {
      case im: IntMessage => node.storage.put(im.value)
      case _ => ???
    }
  }

  override def tick(time: Int, node: Node): Unit = {}
}
