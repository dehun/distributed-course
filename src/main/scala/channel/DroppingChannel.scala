package channel

import scala.util.Random

class DroppingChannel(subchannel:Channel, var dropPattern:Stream[Boolean]) extends Channel {
  val duplicatingChannel = new DuplicatingChannel(subchannel, dropPattern.map(b => if (b) 1 else 0))
  override def send(sender: Channel, msg: Message): Unit = duplicatingChannel.send(sender, msg)
  override def receive() = duplicatingChannel.receive()
}

class RandomlyDroppingChannel(subchannel:Channel)
  extends DroppingChannel(subchannel, Stream.iterate(Random.nextBoolean())((_:Boolean) => Random.nextBoolean())) {
}
