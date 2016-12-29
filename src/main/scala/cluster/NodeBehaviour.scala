package cluster

import channel.{Channel, Message}
import storage.Storage

trait NodeBehaviour {
  def onMessage(sender: Channel, msg: Message, node: Node): Unit
  def tick(time: Int, node: Node): Unit = {}
}
