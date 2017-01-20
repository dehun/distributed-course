import channel._
import cluster.{Cluster, Node}
import org.scalatest.{FlatSpec, Matchers}
import algorithms.ReliableDeliveryBehaviour._
import storage.ReliableStorage


class ReliableDeliveryBehaviourSpec extends FlatSpec with Matchers {
  def testWithChanel(receivingChannel:Channel): Unit = {
    val thingsToDeliver = (1 to 128).toList
    val receiverBehaviour = new Behaviours.Receiver[Int]()
    val receiver = new Node("receiver", receivingChannel, receiverBehaviour,
      new ReliableStorage[Int]())
    val transmitterBehaviour = new Behaviours.Transmitter[Int](thingsToDeliver, receivingChannel)
    val transmitter = new Node("transmitter", new ReliableChannel(), transmitterBehaviour, new ReliableStorage[Int]())
    val cluster = Cluster.fromNodes(List(transmitter, receiver))

    // tick while everything will not be delivered
    var time:Int = 0
    while (!transmitterBehaviour.isEmpty) {
      time += 1
      cluster.tick(time)
    }

    // check that everything has been delivered
    assert (receiverBehaviour.receivedThings.map(_.thing) === thingsToDeliver.toSet)
  }

  "reliable delivery" should "deliver all items over reliable channel" in {
    testWithChanel(new ReliableChannel())
  }

  "reliable delivery" should "deliver all items over dropping channel" in {
    testWithChanel(new RandomlyDroppingChannel(new ReliableChannel()))
  }

  it should "deliver all items over once over duplicating and dropping channel" in {
    testWithChanel(new RandomlyDuplicatingChannel(new ReliableChannel(), (0 to 5)))
  }

  it should "deliver all items over once over reshuffling channel" in {
    testWithChanel(new BufferingChannel(
        new ReshufflingChannel(
          new ReliableChannel()), 10, 150))
  }
}
