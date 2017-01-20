package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}

object ReliableDeliveryBehaviour {
  object Messages {
    case class ReliableMessage[T](id:Int, payload:T) extends Message
    case class Ack(id:Int) extends Message
    case class AckAck(id:Int) extends Message
  }

  object Config {
    val redeliverInterval = 10
  }

  object Behaviours {
    class Transmitter[T](private var thingsToDeliver:List[T], receiver:Channel) extends NodeBehaviour {
      case class InAirMessage(rmsg:Messages.ReliableMessage[T], lastSentAt:Int)
      var inAirMessages = Map.empty[Int, InAirMessage]

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Ack(id:Int) => inAirMessages -= id
      }

      var _nextId = 0
      def nextId(): Int = {
        _nextId += 1
        _nextId
      }

      def sendAndTrack(from:Channel, thing:T, time:Int) = {
        val rmsg = Messages.ReliableMessage(nextId(), thing)
        receiver.send(from, rmsg)
        inAirMessages = inAirMessages.updated(rmsg.id, InAirMessage(rmsg, time))
      }

      override def tick(time: Int, node: Node): Unit = {
        // deliver next message
        thingsToDeliver match {
          case Nil => {} // we have sent everything out, now ensure it's delivered
          case h::t => {
            sendAndTrack(node.input, h, time)
            thingsToDeliver = t
          }
        }
        // redeliver undelivered messages
        inAirMessages.filter(m => time - m._2.lastSentAt > Config.redeliverInterval).foreach(m => {
          receiver.send(node.input, m._2.rmsg)
          inAirMessages = inAirMessages.updated(m._1, InAirMessage(m._2.rmsg, time))
        })
      }

      def isEmpty = thingsToDeliver.isEmpty && inAirMessages.isEmpty
    }

    class Receiver[T] extends NodeBehaviour {
      case class ReceivedThing(id:Int, thing:T) extends Comparable[ReceivedThing] {
        override def compareTo(o: ReceivedThing): Int = id.compare(o.id)
      }

      var receivedThings = Set.empty[ReceivedThing]
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.ReliableMessage(id, payload:T) => {
          sender.send(node.input, Messages.Ack(id))
          receivedThings += ReceivedThing(id, payload)
        }
      }
    }
  }
}
