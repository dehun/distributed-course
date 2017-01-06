package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}
import scala.util.Random


//TODO: move this clock away
class Clock(initialBase:Int) {
  var base = initialBase
  def tell(time:Int) = base + time
  def correct(correction:Int) = base = base + correction
}


object BerkleysTimeSyncBehaviour {
  object Constants {
    val MAX_TIME_ERROR = 100
    val CORRECTION_INTERVAL = 30
  }

  object Messages {
    case class AskTime() extends Message
    case class TellTime(teller:Node.NodeId, time:Int) extends Message
    case class CorrectTime(correction:Int) extends Message
  }

  object Behaviours {
    class Master(slaves: List[Node]) extends NodeBehaviour {
      var times: Map[Node.NodeId, Int] = Map.empty
      var lastAsked = 0

      private def averageTimes = times.values.sum / times.size

      override def init(node: Node): Unit = slaves.foreach(_.input.send(node.input, Messages.AskTime()))

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.TellTime(teller, time) => {
          //Console.println(s"${teller} told ${time}")
          times = times.updated(teller, time)
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        if (time - lastAsked > Constants.CORRECTION_INTERVAL) {
          val averageTime = averageTimes
          times.foreach({ case (nodeId, nodeTime) => {
            // sync node
            val nodeToSync = node.cluster.get.nodes(nodeId)
            val correctionTime = averageTime - nodeTime
            nodeToSync.input.send(node.input, Messages.CorrectTime(correctionTime))
            // ask again
            nodeToSync.input.send(node.input, Messages.AskTime())
          }})
          lastAsked = time
          times = Map.empty
        }
      }
    }

    class Slave extends NodeBehaviour {
      val clock = new Clock(Random.nextInt(Constants.MAX_TIME_ERROR))

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.AskTime() => sender.send(node.input, Messages.TellTime(node.nodeId, clock.tell(time)))
        case Messages.CorrectTime(correction) => {
          clock.correct(correction)
          // Console.println(s"${node.nodeId} corrected clock on ${clock.base} with correction ${correction}")
        }
      }

    }
  }
}
