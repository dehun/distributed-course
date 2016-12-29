package cluster

import storage.Storage
import channel.Channel

object Node {
  type NodeId = String
}

class Node(val nodeId:Node.NodeId, val input:Channel, var behaviour:NodeBehaviour, val storage:Storage[Int]) {
  var cluster:Option[Cluster] = None

  def setCluster(cluster:Cluster):Unit = { this.cluster = Some(cluster) }

  def tick(time:Int) = {
    behaviour.tick(time, this)

    val msg = input.receive()
    if (msg.isDefined) {
      behaviour.onMessage(msg.get.sender, msg.get.msg, this)
    }
  }

}
