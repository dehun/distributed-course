package algorithms

import algorithms.DoublePhaseCommitBehaviour._
import channel._
import cluster._
import org.scalatest._
import storage._

class DoublePhaseCommitSpec extends FlatSpec with Matchers {
  "2pc" should "happy path commit" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))
    // proposer proposes here
    cluster.tick(0)
    // we should have propose messages delivered here, lets process
    cluster.tick(1)
    // we should have accept messages delivered here, lets process
    cluster.tick(2)
    // we should have commit messages delivered here, lets process
    cluster.tick(3)
    // and all nodes should agree upon value 42 now
    assert (acceptorNode1.storage.asList === List(42))
    assert (acceptorNode2.storage.asList === List(42))
    assert (proposerNode.storage.asList === List(42))
  }

  it should "happy path fail" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerRejectBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))
    // proposer proses here
    cluster.tick(0)
    // propose messages delivered, lets process
    cluster.tick(1)
    // accept and reject messages delivered, lets send fail
    cluster.tick(2)
    // process fails
    cluster.tick(3)
    // check that nodes does not have updated storage
    assert (acceptorNode1.storage.size === 0)
    assert (acceptorNode2.storage.size === 0)
    assert (proposerNode.storage.size === 0)
  }

  it should "fail clashing proposals" in {
    val proposerNode1 = new Node("proposer_node_1", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val proposerNode2 = new Node("proposer_node_2", new ReliableChannel(), new ProposeStartBehaviour(13), new ReliableStorage[Any]) // TODO: use slow channel instead
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode1, proposerNode2, acceptorNode1, acceptorNode2))
    // proposer 1 makes a proposal 42
    List(proposerNode1).foreach(_.tick(0))
    // acceptors accept it
    List(acceptorNode1, acceptorNode2).foreach(n => {
      n.processMessages(0)
      n.tick(0)
    })
    // proposer 2 wake ups a bit later on and makes another proposal 13, then rejects proposal from node1
    proposerNode2.processMessages(0)
    proposerNode2.tick(0)
    // proposer1 and proposer 2 process accept messages and reject messages,
    cluster.tick(1)
    // process Fail messages on acceptors
    cluster.tick(2)
    // proposals clashed and both have failed
    assert (acceptorNode1.storage.size === 0)
    assert (acceptorNode2.storage.size === 0)
    assert (proposerNode1.storage.size === 0)
    assert (proposerNode2.storage.size === 0)
  }

  it should "block acceptors if proposer dead" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))
    // proposer sends proposal and dies
    cluster.tick(0)
    proposerNode.behaviour = new DeadNodeBehaviour()
    // acceptors reply with accept
    cluster.tick(1)
    // but proposer is dead
    cluster.tick(2)
    // and will be dead indefinitely
    (3 to 100).foreach(cluster.tick(_))
    // no value has been agreed
    assert (acceptorNode1.storage.size === 0)
    assert (acceptorNode2.storage.size === 0)
    assert (proposerNode.storage.size === 0)
    // and both acceptors still wait for commit
    assert (acceptorNode1.behaviour.isInstanceOf[PeerWaitForCommitBehaviour])
    assert (acceptorNode2.behaviour.isInstanceOf[PeerWaitForCommitBehaviour])
  }

  it should "block proposer if acceptor dies during propose" in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))
    // proposer sends proposal
    proposerNode.tick(0)
    // one acceptor dies
    acceptorNode1.behaviour = new DeadNodeBehaviour()
    // still alive acceptor replies with accept and blocks waiting for commit
    cluster.tick(0)
    // and everything is blocked now
    (1 to 100).foreach(cluster.tick(_))
    // really blocked
    assert (acceptorNode2.behaviour.isInstanceOf[PeerWaitForCommitBehaviour])
    assert (proposerNode.behaviour.isInstanceOf[ProposeWaitAcceptsBehaviour])
  }

  it should "be inconsistent in case if acceptor dies during commit " in {
    val proposerNode = new Node("proposer_node", new ReliableChannel(), new ProposeStartBehaviour(42), new ReliableStorage[Any])
    val acceptorNode1 = new Node("acceptor_node1", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val acceptorNode2 = new Node("acceptor_node2", new ReliableChannel(), new PeerAcceptBehaviour(), new ReliableStorage[Any])
    val cluster = Cluster.fromNodes(List(proposerNode, acceptorNode1, acceptorNode2))
    // proposer sends proposal
    cluster.tick(0)
    // proposer gets all accepts
    cluster.tick(1)
    // one acceptor dies
    acceptorNode1.behaviour = new DeadNodeBehaviour()
    // processing accepts
    cluster.tick(2)
    // sending commits
    cluster.tick(3)
    // but first acceptor dead and has not handled commit properly
    assert (proposerNode.storage.asList == List(42))
    assert (acceptorNode1.storage.asList == List())  // whops, no value!
    assert (acceptorNode2.storage.asList == List(42))
  }
}
