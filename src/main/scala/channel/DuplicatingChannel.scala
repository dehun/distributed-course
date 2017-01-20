package channel

import scala.util.Random

class DuplicatingChannel(subchannel:Channel, var sendPattern:Stream[Int]) extends Channel {
  override def send(sender: Channel, msg: Message): Unit = {
    val h = sendPattern.head
    sendPattern = sendPattern.tail
    (1 to h).foreach(_ => subchannel.send(sender, msg))
  }

  override def receive() = subchannel.receive()
}

object RandomlyDuplicatingChannel {
  def randomPick(range: Seq[Int]) = range(Math.abs(Random.nextInt()) % range.size)
}

class RandomlyDuplicatingChannel(subchannel:Channel, range:Seq[Int])
  extends DuplicatingChannel(subchannel,
    Stream.iterate(RandomlyDuplicatingChannel.randomPick(range))(
    (_:Int) => RandomlyDuplicatingChannel.randomPick(range)))
