import org.scalatest._

import algorithms.BerkleysTimeSyncBehaviour._
import cluster._
import channel._
import storage._

class BerkleysTimeSyncBehaviourSpec extends FlatSpec with Matchers {
  "berkley time sync" should "sync times" in {
    val slaves = (1 to 20).map(i => new Node("slave_" + i, new ReliableChannel(),
      new Behaviours.Slave(), new ReliableStorage[Int])) toList
    val master = new Node("master", new ReliableChannel(), new Behaviours.Master(slaves), new ReliableStorage[Int])
    val cluster = Cluster.fromNodeList(master::slaves)
    // let some time pass
    (1 to 1000).foreach(cluster.tick)
    // all slaves clocks should be synchronized now
    val clocks = slaves.map(_.behaviour.asInstanceOf[Behaviours.Slave].clock)
    clocks.foreach(c => assert (c.tell(1001) === clocks.head.tell(1001)))
  }

}
