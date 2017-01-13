import channel.ReliableChannel
import cluster.Node
import cluster.Cluster
import org.scalatest._
import storage.ReliableStorage
import algorithms.BullyBehaviour._

import scala.util.Random

class BullyBehaviourSpec extends FlatSpec with Matchers {
  "bully" should "have only one bully at the end" in {
    val nodeUids = Random.shuffle(1 to 100)
    def uidToName(uid:Int) = "node_" + uid
    val allNodeNames:Set[String] = nodeUids.map("node_" + _) toSet
    val nodes = nodeUids.map(uid => new Node(uidToName(uid), new ReliableChannel(),
      new Behaviours.Bully(uid, allNodeNames - uidToName(uid)), // bully everyone except us
      new ReliableStorage[Int]()))
    val cluster = Cluster.fromNodes(nodes toList)
    // tick cluster
    (1 to 100).foreach(cluster.tick)
    // only one bully remains
    val bullies = nodes.filter(_.behaviour.isInstanceOf[Behaviours.Bully])
    assert (bullies.size === 1)
    assert (bullies.head.behaviour.asInstanceOf[Behaviours.Bully].nodesToBully === Set.empty)
  }
}
