import algorithms.DhtBehaviour.Behaviours.QueryResult
import org.scalatest._
import channel._
import cluster._
import storage._
import algorithms.DhtBehaviour._

class DhtBehaviourSpec extends FlatSpec with Matchers {
  "dht" should "store all values on right nodes" in {
    val valuesToStore = (1 to 99)
    val backendNodes = (1 to 3).map(i => new Node("backend_" + i, new ReliableChannel(),
      new Behaviours.BackendNode(), new ReliableStorage[Int])) toList
    val clientNode = new Node("client_node", new ReliableChannel(),
      new Behaviours.Client(valuesToStore toStream, backendNodes),
      new ReliableStorage[Int])
    val cluster = Cluster.fromNodeList(clientNode :: backendNodes)
    // tick cluster - client will need around 100 ticks to send all data
    (1 to 200).foreach(cluster.tick)
    // check that we have all values evenly distributed
    assert (backendNodes.forall(_.storage.size === 33))
    // check that we have all values
    assert (backendNodes.flatMap(_.storage.asList).sorted == valuesToStore)
  }

  it should "be possible to query values" in {
    val valuesToStore = (1 to 99)
    val backendNodes = (1 to 3).map(i => new Node("backend_" + i, new ReliableChannel(),
      new Behaviours.BackendNode(), new ReliableStorage[Int])) toList
    val clientNode = new Node("client_node", new ReliableChannel(),
      new Behaviours.Client(valuesToStore toStream, backendNodes),
      new ReliableStorage[Int])
    val cluster = Cluster.fromNodeList(clientNode :: backendNodes)
    // tick cluster - client will need around 100 ticks to send all data
    (1 to 200).foreach(cluster.tick)
    // check that we have all values evenly distributed
    assert (backendNodes.forall(_.storage.size === 33))
    // check that we have all values
    assert (backendNodes.flatMap(_.storage.asList).sorted == valuesToStore)
    // now turn client to querying client and go
    clientNode.behaviour = new Behaviours.QueryingClient(Stream.cons(100, valuesToStore toStream), backendNodes)
    (201 to 400).foreach(cluster.tick)
    // check the results
    clientNode.behaviour.asInstanceOf[Behaviours.QueryingClient].results.forall({
      case QueryResult(value, whereopt) => whereopt match {
        case Some(where) => cluster.nodes(where).storage.asList.contains(value)
        case None => !valuesToStore.contains(value)
      }
    })
  }
}
