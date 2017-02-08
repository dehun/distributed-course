package algorithms

import org.scalatest.{FlatSpec, Matchers}
import algorithms.SingleDecreePaxosBehaviour._
import channel.{BufferingChannel, ReliableChannel, ReshufflingChannel}
import cluster.{Cluster, Node}
import storage.ReliableStorage

class SingleDecreePaxosBehaviourSpec extends FlatSpec with Matchers {
  "single decree paxos" should "agree on value with single proposer" in {
    val acceptorNames = (1 to 3).map(i => "acceptor_" + i).toSet
    val valueToAgree = "asdawqedsacxz"
    val proposer = new Node("proposer", new ReliableChannel(),
      new Behaviors.Proposer(acceptorNames, valueToAgree, new ProposalNumberGenerator()),
      new ReliableStorage[Any]())
    val acceptors = acceptorNames.map(a =>
      new Node(a, new ReliableChannel(), new Behaviors.Acceptor(), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(proposer::acceptors.toList)
    // tick cluster
    (1 to 100).foreach(cluster.tick)
    // value should be chosen
    val majorityCount = acceptors.size / 2 + 1
    assert (acceptors.count(a => a.storage.last.exists(_.asInstanceOf[String] == valueToAgree)) > majorityCount)
  }

  it should "agree on value with two proposers" in {
    val acceptorNames = (1 to 3).map(i => "acceptor_" + i).toSet
    val valueToAgreeA = "A"
    val valueToAgreeB = "B"
    val proposerA = new Node("proposerA", new ReliableChannel(),
      new Behaviors.Proposer(acceptorNames, valueToAgreeA, new ProposalNumberGenerator()),
      new ReliableStorage[Any]())
    val proposerB = new Node("proposerB", new ReliableChannel(),
      new Behaviors.Proposer(acceptorNames, valueToAgreeB, new ProposalNumberGenerator()),
      new ReliableStorage[Any]())
    val acceptors = acceptorNames.map(a =>
      new Node(a, new ReliableChannel(), new Behaviors.Acceptor(), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(proposerA::proposerB::acceptors.toList)
    // tick cluster
    (1 to 100).foreach(cluster.tick)
    // value should be chosen
    val majorityCount = acceptors.size / 2 + 1
    assert (acceptors.count(a => a.storage.last.exists(_.asInstanceOf[String] == valueToAgreeA)) > majorityCount)
  }

  def newChannelFromHell() =
    new BufferingChannel(
        new ReshufflingChannel(
          new ReliableChannel()), 2, 10)

  it should "agree on value with single proposer and tricky channel" in {
    val acceptorNames = (1 to 3).map(i => "acceptor_" + i).toSet
    val valueToAgree = "asdawqedsacxz"
    val proposer = new Node("proposer", newChannelFromHell(),
      new Behaviors.Proposer(acceptorNames, valueToAgree, new ProposalNumberGenerator()),
      new ReliableStorage[Any]())
    val acceptors = acceptorNames.map(a =>
      new Node(a, newChannelFromHell(), new Behaviors.Acceptor(), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(proposer::acceptors.toList)
    // tick cluster
    (1 to 100).foreach(cluster.tick)
    // value should be chosen
    val majorityCount = acceptors.size / 2 + 1
    assert (acceptors.count(a => a.storage.last.exists(_.asInstanceOf[String] == valueToAgree)) > majorityCount)
  }
}
