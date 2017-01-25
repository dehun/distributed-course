package cluster

import channel._
import org.scalatest._
import storage._

class SimpleBehaviourTest extends FlatSpec with Matchers {
  "dead node" should "be dead" in {
    val node1 = new Node("node1", new ReliableChannel(), new DeadNodeBehaviour(), new ReliableStorage[Any])
    node1.tick(0)
    val ch = new ReliableChannel()
    node1.input.send(ch, IntMessage(42))
    assert(ch.receive() === None)
  }

  "echo node" should "echo" in {
    val node1 = new Node("node1", new ReliableChannel(), new EchoBehaviour(), new ReliableStorage[Any])
    val ch = new ReliableChannel()
    node1.input.send(ch, IntMessage(42))
    node1.tick(0)
    node1.processMessages(0)
    assert(ch.receive().get.msg === IntMessage(42))
    assert(ch.receive() === None)
  }

  "accept everything" should "accept message" in {
    val node1 = new Node("node1", new ReliableChannel(), new AcceptEverythingBehaviour(), new ReliableStorage[Any])
    val ch = new ReliableChannel()
    node1.input.send(ch, IntMessage(42))
    node1.tick(0)
    node1.storage.size === 1
    node1.storage.get(0) === 43
    ch.receive() === None
  }
}
