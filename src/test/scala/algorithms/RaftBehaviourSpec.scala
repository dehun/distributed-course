package algorithms

import algorithms.RaftBehaviour._
import org.scalatest._
import channel._
import cluster._
import storage._

class RaftBehaviourSpec extends FlatSpec with Matchers {
  "raft" should "elect a leader after start" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, new ReliableChannel(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 500).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
  }

  it should "re-elect a leader after leader death" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, new ReliableChannel(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 500).foreach(cluster.tick)
    // leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // kill a leader
    leaders.head.behaviour = new DeadNodeBehaviour()
    // and elect a new one
    (501 to 1000).foreach(cluster.tick)
    // leader should be elected by now
    assert (raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader]).size === 1)
  }
}