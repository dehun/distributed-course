package algorithms

import algorithms.RaftBehaviour._
import org.scalatest._
import channel._
import cluster._
import storage._

class RaftBehaviourSpec extends FlatSpec with Matchers {
  def newChannelFromHell() =
    new IfChannel(msg => {msg.isInstanceOf[Messages.ClientPut.Request]},
      new ReliableChannel(),
      new BufferingChannel(
      new ReshufflingChannel(
        new ReliableChannel()), 2, 10))

  "raft" should "elect a leader after start" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, newChannelFromHell(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 1000).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
  }

  it should "re-elect a leader after leader death" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, newChannelFromHell(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 1000).foreach(cluster.tick)
    // leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // kill a leader
    leaders.head.behaviour = new DeadNodeBehaviour()
    // and elect a new one
    (1001 to 2000).foreach(cluster.tick)
    // leader should be elected by now
    assert (raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader]).size === 1)
  }

  it should "store single pushed value" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, newChannelFromHell(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 500).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // send append to leader
    val input = new ReliableChannel()
    val leader = leaders.head
    leader.input.send(input, Messages.ClientPut.Request("babaka"))
    // expect Reply message with success
    val trecv = (501 to 1000).find(t => {
      cluster.tick(t)
      val msg = input.receive()
      Console.println(msg)
      msg.exists(_.msg.isInstanceOf[Messages.ClientPut.Reply])
    })
    // should be on majority of servers now
    val replicasCount = raftNodes.count(n => n.storage.last.get.asInstanceOf[RaftLogEntry].value == "babaka")
    assert (replicasCount >= raftNodes.size / 2 + 1)
    // the reply messages should be there at some point
    assert(trecv.isDefined)
  }

  it should "store few pushed at the same time values" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, newChannelFromHell(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 1000).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // send append to leader
    val input = new ReliableChannel()
    val leader = leaders.head
    (1 to 5).foreach(i => leader.input.send(input, Messages.ClientPut.Request("babaka_" + i)))

    // receive replies
    var currentTime = 1001
    for {i <- 1 to 5}
    {
      // expect Reply message with success
      val trecv = (currentTime to 3000).find(t => {
        cluster.tick(t)
        val msg = input.receive()
        Console.println(msg)
        msg.exists(_.msg.isInstanceOf[Messages.ClientPut.Reply])
      })
      // should be on majority of servers now
      val replicasCount = raftNodes.count(n => n.storage.asList.map(_.asInstanceOf[RaftLogEntry])
        .exists(_.value == ("babaka_" + i)))
      assert(replicasCount >= raftNodes.size / 2 + 1)
      // the reply messages should be there at some point
      assert(trecv.isDefined)
      currentTime = trecv.get
    }
    // storage should be 5
    raftNodes.foreach(n => assert (n.storage.size === 6)) // we have a placeholder in there, so 4 + 1
  }

  it should "store several pushed values" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, new ReliableChannel(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 500).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // send append to leader
    val input = new ReliableChannel()
    val leader = leaders.head
    var currentTime = 501

    for {i <- 1 to 10}
    {
       leader.input.send(input, Messages.ClientPut.Request("babaka" + i))
      // expect Reply message with success
      val trecv = (currentTime to 2000).find(t => {
        cluster.tick(t)
        val msg = input.receive()
        Console.println(msg)
        msg.exists(_.msg.isInstanceOf[Messages.ClientPut.Reply])
      })
      // should be on majority of servers now
      val replicasCount = raftNodes.count(n => n.storage.last.get.asInstanceOf[RaftLogEntry].value == ("babaka" + i))
      assert(replicasCount >= raftNodes.size / 2 + 1)
      // the reply messages should be there at some point
      assert(trecv.isDefined)
      currentTime = trecv.get
    }
    // storage should be 5
    raftNodes.foreach(n => assert (n.storage.size === 11)) // we have a placeholder in there, so 4 + 1
  }

  it should "store single pushed value after leader failure replicating previous one" in {
    val raftNodeNames = (1 to 5).map(n => s"raft_${n}").toSet
    val raftNodes = raftNodeNames.map(n =>
      new Node(n, newChannelFromHell(), new Behaviours.Follower(raftNodeNames), new ReliableStorage[Any]()))
    val majorityCount = raftNodes.size / 2 + 1
    val cluster = Cluster.fromNodes(raftNodes)
    // tick
    (1 to 500).foreach(cluster.tick)
    // new leader should be elected by now
    val leaders = raftNodes.filter(_.behaviour.isInstanceOf[Behaviours.Leader])
    assert (leaders.size === 1)
    // send append to leader
    val input1 = new ReliableChannel()
    val input2 = new ReliableChannel()
    val leader = leaders.head
    leader.input.send(input1, Messages.ClientPut.Request("babaka"))
    cluster.tick(501)
    // and fail leader
    leader.behaviour = new DeadNodeBehaviour()
    // wait for new leader to emerge
    val newLeaderTime = (501 to 1000).find(t => {
      cluster.tick(t)
      raftNodes.exists(_.behaviour.isInstanceOf[Behaviours.Leader])
    })
    assert (newLeaderTime.isDefined)
    // and send another one request
    raftNodes.find(_.behaviour.isInstanceOf[Behaviours.Leader])
      .head.input.send(input2, Messages.ClientPut.Request("sobaka"))
    // wait for request to be finished
    val replyTime = (newLeaderTime.get to 2000).find(t => {
      cluster.tick(t)
      input2.receive().isInstanceOf[Messages.ClientPut.Reply]}
    )
    // we should get sobaka at majority now
    assert (raftNodes.count(_.storage.asList.map(_.asInstanceOf[RaftLogEntry]).last.value == "sobaka") >= majorityCount)
  }
}
