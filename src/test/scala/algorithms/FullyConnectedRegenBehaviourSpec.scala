package algorithms

import algorithms.FullyConnectedRegenBehaviour._
import channel._
import cluster._
import org.scalatest._
import storage._

class FullyConnectedRegenBehaviourSpec extends FlatSpec with Matchers {
  "fully connected regen" should "work just fine without link corruptions" in {
    val nodeNames = (1 to 5).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 1000).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
  }

  it should "evict dead node" in {
    val nodeNames = (1 to 10).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 1000).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
    // kill one node
    nodes.head.behaviour = new DeadNodeBehaviour()
    // tick nodes
    (1001 to 2000).foreach(cluster.tick)
    // this node should be dead everywhere except itself
    nodes.tail.foreach(n => assert (n.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === (nodeNames - nodes.head.nodeId)))
  }

  it should "evict few dead nodes" in {
    val nodeNames = (1 to 10).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 1000).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
    // kill one node
    nodes.head.behaviour = new DeadNodeBehaviour()
    nodes.tail.head.behaviour = new DeadNodeBehaviour()
    val stillAliveNodes = nodes.tail.tail.map(_.nodeId).toSet
    // tick nodes
    (1001 to 2000).foreach(cluster.tick)
    // this node should be dead everywhere except itself
    nodes.tail.tail.foreach(n => assert (n.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === stillAliveNodes))
  }

  it should "evict dead node optimally when we have broken connection" in {
    val nodeNames = (1 to 3).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 501).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
    // kill channel between node 2 and node 3
    cluster.nodes("node_3").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_2"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    cluster.nodes("node_2").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_3"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    // let em detect that broken link
    (501 to 1000).foreach(cluster.tick)
    // node 2 should be evicted from all nodes as its id is lower
    Set(cluster.nodes("node_1"), cluster.nodes("node_3")).foreach(
      n => assert (n.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === Set("node_1", "node_3")))
  }

  it should "evict dead node optimally when we have few broken connections" in {
    val nodeNames = (1 to 3).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 501).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
    // kill channel between node 2 and node 3
    cluster.nodes("node_3").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => Set("node_1", "node_2").contains(senderId)
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    cluster.nodes("node_2").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_3"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    cluster.nodes("node_1").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_3"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    // let em detect that broken link
    (501 to 1000).foreach(cluster.tick)
    // node 3 should be evicted from all nodes
    Set(cluster.nodes("node_1"), cluster.nodes("node_2")).foreach(
      n => assert (n.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === Set("node_1", "node_2")))
  }

  //  n1 ----- n2
  //  | \     / |
  //  |    \/   x
  //  |  /    \ |
  //  n3 ----- n4

  it should "evict dead node optimally when we have broken connection in 4 nodes cluster" in {
    val nodeNames = (1 to 4).map("node_" + _).toSet
    val nodes = nodeNames.map(name => new Node(name, new ReliableChannel(),
      new Behaviours.FullyConnectedRegen(nodeNames),
      new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // tick nodes
    (1 to 501).foreach(cluster.tick)
    // all nodes should be alive at all nodes
    assert(nodes.forall(_.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === nodeNames))
    // kill channel between nodes 2 and 4
    cluster.nodes("node_2").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_4"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    cluster.nodes("node_4").input = new IfChannel(
      {
        case Messages.Heartbeat(senderId, _) => senderId == "node_2"
        case _ => false
      },
      new DeadChannel(),
      new ReliableChannel())
    // let em detect that broken link
    (501 to 1000).foreach(cluster.tick)
    // node 2 should be evicted from all nodes as its id is lower
    Set(cluster.nodes("node_1"), cluster.nodes("node_3"), cluster.nodes("node_4")).foreach(
      n => assert (n.behaviour.asInstanceOf[Behaviours.FullyConnectedRegen].aliveNodes === Set("node_1", "node_3", "node_4")))
  }
}
