package cluster

import storage.Storage
import channel.{Channel, Message}

object Node {
  type NodeId = String
}

class Node(val nodeId:Node.NodeId, var input:Channel, var behaviour:NodeBehaviour, val storage:Storage[Any]) {
  var cluster:Option[Cluster] = None

  def setCluster(cluster:Cluster):Unit = { this.cluster = Some(cluster) }

  def init() = behaviour.init(this)

  def tick(time:Int) = {
    behaviour.tick(time, this)
  }

  def processMessages(time:Int) = {
    var msg = input.receive()
    while (msg.isDefined) {
      behaviour.onMessage(msg.get.sender, msg.get.msg, this, time)
      msg = input.receive()
    }
  }
}
