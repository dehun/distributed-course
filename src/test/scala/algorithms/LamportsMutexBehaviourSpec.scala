package algorithms

import algorithms.LamportsMutexBehavior._
import channel.ReliableChannel
import cluster.{Cluster, Node}
import org.scalatest.{FlatSpec, Matchers}
import storage.ReliableStorage

import scala.collection.mutable.MutableList

class LamportsMutexBehaviourSpec extends FlatSpec with Matchers {
  "Lamport's mutex" should "grant all nodes lock at some point" in {

    val lockers = MutableList[(Int, Node.NodeId)]()

    val nodes = (1 to 20).map(nid =>
      new Node("node_" + nid,
        new ReliableChannel(),
        new Mutex((time:Int, node:Node) => { lockers += ((time, node.nodeId)) },
          0,
          Set(LockRequest(-1, "test"))), // first test is locked the mutex
        new ReliableStorage[Any]))
    val cluster = Cluster.fromNodes(nodes)
    // unlock the mutex
    nodes.foreach(n => n.input.send(n.input, Messages.LmMessage(1, "test", Messages.ReleaseOp())))

    // now lock it arbitary amount of times
    (1 to 500).foreach(cluster.tick)
    // lockers should contain all nodes
    nodes.foreach(node => assert (lockers.exists(_._2 == node.nodeId)))
    // lockers should all be at different times
    lockers.foreach(l => assert (lockers.count(_._1 == l._1) === 1))
  }

}
