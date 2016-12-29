package cluster

import channel.Message
import cluster.Node.NodeId


object Cluster {
  def fromNodeList(nodes:List[Node]):Cluster = {
    new Cluster(nodes.map(n => n.nodeId -> n).toMap)
  }
}


class Cluster(val nodes:Map[Node.NodeId, Node]) {
  nodes.foreach(_._2.setCluster(this))

  def tick(time:Int):Unit = {
    nodes.values.foreach(_.tick(time))
  }

  def multicastExceptMe(me:Node, msg:Message): Unit = {
    nodes.filter(_._1 != me.nodeId).values.map(_.input.send(me.input, msg))
  }
}
