import algorithms.ChangAndRobertsBehaviour._
import channel.ReliableChannel
import cluster.{Cluster, Node}
import org.scalatest.{FlatSpec, Matchers}
import storage.ReliableStorage
import scala.util.Random

class ChangAndRobertsSpec extends FlatSpec with Matchers {
  "Chang and roberts leader election" should "elect a leader" in {
    val nodeIds = Random.shuffle(1 to 10) toList
    // (node_1, node_2), (node_2, node_3), ... (node_10, node_1)
    val pairs = nodeIds.init.zip(nodeIds.tail) ++ List((nodeIds.last, nodeIds.head))
    val cluster = Cluster.fromNodes(
      pairs.map({case (n1, n2) => new Node("node_" + n1, new ReliableChannel(),
        { if (Random.nextBoolean()) new Behaviour.NonParticipant(n1, "node_" + n2) // half nodes are initiators
          else new Behaviour.NonParticipantInitiator(n1, "node_" + n2) },
        new ReliableStorage[Int]())}) toList)
    // elect a leader
    (1 to 1000).foreach(cluster.tick)
    // find a leader
    val leaders = cluster.nodes.values.filter(_.behaviour.isInstanceOf[Behaviour.Leader])
    assert (leaders.size === 1)
    val leader = leaders.head
    assert (leader.behaviour.asInstanceOf[Behaviour.Leader].uid === nodeIds.max)
  }
}
