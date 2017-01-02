import org.scalatest._
import algorithms.GossipBehaviour._
import channel._
import cluster._
import storage._

import scala.util.Random


class GossipBehaviourSpec extends FlatSpec with Matchers {
  "gossip" should "work just well when all nodes know each other" in {
    val nodeNames = (1 to 4).map("node_" + _).toSet
    // all nodes know each other from the startpu
    val cluster = Cluster.fromNodeList(
      nodeNames.map(n => new Node(n, new ReliableChannel(), new GossipBehaviour(nodeNames), new ReliableStorage[Int]())) toList)
    // tick cluster hundred times
    (1 to 100).foreach(cluster.tick)
    // and ensure that all nodes still know each other
    assert(cluster.nodes.values.forall(n => n.behaviour.asInstanceOf[GossipBehaviour].knowledge(n.nodeId) == nodeNames))
  }

  it should "work when every node know only next node (circular)" in {
    val nodeNames = (1 to 100).map("node_" + _)
    // at first nodes know only one other node
    // (node_1, node_2), (node_2, node_3), ... (node_10, node_1)
    val pairs = nodeNames.init.zip(nodeNames.tail) ++ List((nodeNames.last, nodeNames.head))
    val cluster = Cluster.fromNodeList(
      pairs.map({case (n1, n2) => new Node(n1, new ReliableChannel(), new GossipBehaviour(Set(n1, n2)), new ReliableStorage[Int]())}) toList)
    // how much ticks it will take to all nodes to get know each other?
    (1 to 10).foreach(cluster.tick)
    // and how much messages?
    assert (Messages.Gossip.instantiations === 22812)
    // all nodes know each other
    assert(cluster.nodes.values.forall(n => n.behaviour.asInstanceOf[GossipBehaviour].knowledge(n.nodeId) === nodeNames.toSet))
  }

  it should "isolated nodes never get known" in {
    val nonIsolatedNodeNames = (1 to 9).map("node_" + _)
    // at first nodes know only one other node
    // (node_1, node_2), (node_2, node_3), ... (node_10, node_1)
    val pairs = nonIsolatedNodeNames.init.zip(nonIsolatedNodeNames.tail) ++ List((nonIsolatedNodeNames.last, nonIsolatedNodeNames.head))
    val isolatedNodeName = "isolated_node"
    val isolatedNode = new Node(isolatedNodeName, new ReliableChannel(), new GossipBehaviour(Set(isolatedNodeName)), new ReliableStorage[Int]())
    val nonIsolatedNodes = pairs.map({case (n1, n2) => new Node(n1, new ReliableChannel(), new GossipBehaviour(Set(n1, n2)), new ReliableStorage[Int]())}) toList
    val cluster = Cluster.fromNodeList(isolatedNode :: nonIsolatedNodes)
    // tick nodes
    (1 to 10).foreach(cluster.tick)
    // all nodes know each other
    assert (isolatedNode.behaviour.asInstanceOf[GossipBehaviour].knowledge == Map(isolatedNodeName -> Set(isolatedNodeName)))
    assert(nonIsolatedNodes.forall(n => n.behaviour.asInstanceOf[GossipBehaviour].knowledge(n.nodeId) === nonIsolatedNodeNames.toSet))
  }

  it should "gossiping over unreliable channel requires different approach" in {
    val nodeNames = (1 to 10).map("node_" + _)
    // at first nodes know only one other node, circular (for fun)
    // (node_1, node_2), (node_2, node_3), ... (node_10, node_1)
    val pairs = nodeNames.init.zip(nodeNames.tail) ++ List((nodeNames.last, nodeNames.head))
    val cluster = Cluster.fromNodeList(
      pairs.map({case (n1, n2) => new Node(n1,
        new DroppingChannel(new ReliableChannel(),Stream.iterate(Random.nextBoolean())((_:Boolean) => Random.nextBoolean())), // 50% chance of drop
        new ReliableGossipBehaviour(Set(n1, n2)), // we are using special gossiping behaviour
        new ReliableStorage[Int]())}) toList) // require reliable storage!
    // how much ticks it will take to all nodes to get know each other?
    (1 to 500).foreach(cluster.tick)
    // all nodes know each other
    assert(cluster.nodes.values.forall(n => n.behaviour.asInstanceOf[ReliableGossipBehaviour].knowledge(n.nodeId) === nodeNames.toSet))
  }

}
