package algorithms

import algorithms.LamportsMutex.Messages.{LmMessage, LmOp}
import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}


object LamportsMutex {
  object Messages {
    class LmOp()
    case class RequestOp() extends LmOp
    case class ReleaseOp() extends LmOp
    case class ReplyOp() extends LmOp
    case class LmMessage(lmstamp:Int, senderId:Node.NodeId, op:LmOp) extends Message
  }

  abstract class LmBehaviour(protected var ourLmstamp:Int) extends NodeBehaviour {
    def onLmMessage(sender: Channel, msg: Messages.LmMessage, node: Node, time: Int)

    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
      case lmmsg:LmMessage => {
        ourLmstamp = Math.max(ourLmstamp, lmmsg.lmstamp) + 1
        Console.println(s"${node.nodeId} got ${lmmsg.op} with stamp ${lmmsg.lmstamp}, our lmstamp is ${ourLmstamp}")
        onLmMessage(sender, lmmsg, node, time)
      }
    }

    def lmsend(senderId:Node.NodeId, from:Channel, to:Channel, op:LmOp) = {
      ourLmstamp += 1
      Console.println(s"${senderId} sending ${op} with stamp ${ourLmstamp}")
      to.send(from, LmMessage(ourLmstamp, senderId, op))
    }
  }

  case class LockRequest(val lmstamp:Int, val requesterId:Node.NodeId)

  class Mutex(onLock:Function2[Int, Node, Unit],
              private val ourStartLmstamp:Int,
              private var requestQueue:Set[LockRequest]) extends LmBehaviour(ourStartLmstamp) {

    override def onLmMessage(sender: Channel, msg: LmMessage, node: Node, time: Int): Unit = {
      msg.op match {
        case Messages.RequestOp() => {
          requestQueue += LockRequest(msg.lmstamp, msg.senderId)
          lmsend(node.nodeId, node.input, sender, Messages.ReplyOp())
        }
        case Messages.ReplyOp() => { }
        case Messages.ReleaseOp() =>
          requestQueue = requestQueue.filterNot(_.requesterId == msg.senderId)
      }
    }

    override def tick(time: Int, node: Node): Unit = {
      lock(node)
    }

    private def lock(node:Node) = {
      val otherNodes = node.cluster.get.nodes.values.filterNot(_.eq(node))
      otherNodes.foreach((n) => {
        lmsend(node.nodeId, node.input, n.input, Messages.RequestOp())
      })
      requestQueue = requestQueue + LockRequest(ourLmstamp, node.nodeId)
      node.behaviour = new MutexLocker(onLock, ourLmstamp, requestQueue,
        otherNodes.map(_.nodeId).toSet)
    }
  }

  class MutexLocker(onLock:Function2[Int, Node, Unit],
                    private val ourStartLmstamp:Int,
                    private var requestQueue:Set[LockRequest],
                    private var repliesToReceive:Set[Node.NodeId]
                   ) extends LmBehaviour(ourStartLmstamp) {
    override def onLmMessage(sender: Channel, msg: Messages.LmMessage, node: Node, time: Int): Unit = {
      msg.op match {
        case Messages.ReplyOp() => {
          if (ourStartLmstamp > msg.lmstamp) {
            Console.println(s"${node.nodeId} ignores ${msg}")
          } // ignore old replies
          repliesToReceive = repliesToReceive - msg.senderId
        }

        case Messages.RequestOp() => {
          requestQueue += LockRequest(msg.lmstamp, msg.senderId)
          lmsend(node.nodeId, node.input, sender, Messages.ReplyOp())
        }

        case Messages.ReleaseOp() => {
          requestQueue = requestQueue.filterNot(_.requesterId == msg.senderId)
        }
      }
    }

    override def tick(time: Int, node: Node): Unit = {
      val firstToLock = requestQueue.minBy(r => (r.lmstamp, r.requesterId))
      if (repliesToReceive.isEmpty &&
        firstToLock.requesterId == node.nodeId) {
        Console.println(s"${node.nodeId} locks mutex at time ${time} with request queue ${requestQueue.toList.sortBy((r) => (r.lmstamp, r.requesterId))}")
        onLock(time, node)
        // release
        requestQueue = requestQueue.filterNot(_.requesterId == node.nodeId)
        node.cluster.get.nodes.values.filterNot(_.eq(node)).foreach((n) => {
          lmsend(node.nodeId, node.input, n.input, Messages.ReleaseOp())
        })
        node.behaviour = new Mutex(onLock, ourLmstamp, requestQueue)
      }
    }
  }
}
