import org.scalatest._

import channel._
import cluster._
import storage._
import algorithms.TriplePhaseCommitBehaviour.Behaviours._


class TriplePhaseCommitSpec extends FlatSpec with Matchers {
  "3pc" should "work in happy path" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposerStart(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))

    // send proposal
    cluster.tick(0)
    // process proposal and send accepts
    cluster.tick(1)
    // process accepts and send precommit
    cluster.tick(2)
    // process precommit and send commits
    cluster.tick(3)
    // handle commits
    cluster.tick(4)
    // check that value is arrived at all the nodes
    assert (acceptorNode1.storage.asList == List(42))
    assert (acceptorNode2.storage.asList == List(42))
    assert (proposerNode.storage.asList == List(42))
  }

  it should "work in happy path with transaction reject" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposerStart(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new RejectorStart(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))

    // send proposal
    cluster.tick(0)
    // process proposal and send accept and reject
    cluster.tick(1)
    // process reject and send fail to acceptors
    cluster.tick(2)
    // handle fail
    cluster.tick(3)
    // value has not arrived, even after several ticks has passed
    (4 to 100).foreach(cluster.tick)
    // check that value has been accepted at any of nodes
    assert (acceptorNode1.storage.asList == List())
    assert (acceptorNode2.storage.asList == List())
    assert (proposerNode.storage.asList == List())
  }

  it should "work witch watchdog when initiator is ok" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposerStart(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])

    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])
    val watchdogNode = new Node("watchdog_node2", new ReliableChannel(), new WatchdogStart(), new ReliableStorage[Any]())

    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2, watchdogNode))

    // send proposal
    cluster.tick(0)
    // process proposal and send accepts
    cluster.tick(1)
    // process accepts and send precommit
    cluster.tick(2)
    // process precommit and send commits
    cluster.tick(3)
    // handle commits
    cluster.tick(4)
    // check that value is arrived at all the nodes
    assert (acceptorNode1.storage.asList == List(42))
    assert (acceptorNode2.storage.asList == List(42))
    assert (proposerNode.storage.asList == List(42))
    // and watchdog does not save anything in our scenario, but it technically can
  }

  it should "work witch watchdog when initiator dies" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposerStart(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])

    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new AcceptorStart(), new ReliableStorage[Any])
    val watchdogNode = new Node("watchdog_node2", new ReliableChannel(), new WatchdogStart(), new ReliableStorage[Any]())

    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2, watchdogNode))

    // send proposal
    cluster.tick(0)
    // process proposal and send accepts
    cluster.tick(1)
    // process accepts and send precommit
    cluster.tick(2)
    // kill proposer
    proposerNode.behaviour = new DeadNodeBehaviour()
    // trigger watchdog
    (4 to 50).foreach(cluster.tick)
    // check that value is arrived at all the nodes
    assert (acceptorNode1.storage.asList == List(42))
    assert (acceptorNode2.storage.asList == List(42))
    assert (proposerNode.storage.asList == List()) // because it was dead
    // and watchdog does not save anything in our scenario, but it technically can
  }

}
