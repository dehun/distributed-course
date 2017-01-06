package algorithms

import algorithms.TriplePhaseCommitBehaviour.Messages.Heartbeat
import channel.{Channel, Message}
import cluster.{DeadNodeBehaviour, Node, NodeBehaviour}

object TriplePhaseCommitBehaviour {
  object Messages {
    case class Propose(value: Int, initiator:String) extends Message
    case class Accept() extends Message
    case class Reject() extends Message
    case class PreCommit() extends Message
    case class PreCommitAck() extends Message
    case class Commit() extends Message
    case class Fail() extends Message
    case class Heartbeat() extends Message
  }

  object Behaviours {
    // proposer
    class ProposerStart(proposal:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = ???
      override def tick(time: Int, node: Node): Unit = {
        node.behaviour = new ProposerWaitForAccepts(proposal, node.cluster.get.nodes.size - 1)
        node.cluster.get.multicastExceptMe(node, Messages.Propose(proposal, node.nodeId))
      }
    }

    class ProposerWaitForAccepts(value:Int, acksToPreCommit:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Accept() =>
          if (acksToPreCommit > 1) {
            node.behaviour = new ProposerWaitForAccepts(value, acksToPreCommit - 1)
          } else {
            node.behaviour = new ProposerWaitForPreCommitAcks(value, node.cluster.get.nodes.size - 1)
            node.cluster.get.multicastExceptMe(node, Messages.PreCommit())
          }

        case Messages.Reject() => {
          node.behaviour = new DeadNodeBehaviour()
          node.cluster.get.multicastExceptMe(node, Messages.Fail())
        }
      }
    }

    class ProposerWaitForPreCommitAcks(value:Int, acksToCommit:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.PreCommitAck() =>
          if (acksToCommit > 1) {
            node.behaviour = new ProposerWaitForPreCommitAcks(value, acksToCommit - 1)
          } else {
            node.behaviour = new ProposerCommits(value)
          }


        case Messages.Heartbeat() => {
          sender.send(node.input, Messages.Heartbeat())
        }
      }
    }

    class ProposerCommits(value:Int) extends NodeBehaviour {
      override def tick(time: Int, node: Node): Unit = {
        node.cluster.get.multicastExceptMe(node, Messages.Commit())
        node.storage.put(value)
        node.behaviour = new JustHeartbeats()
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Heartbeat() => {
          sender.send(node.input, Messages.Heartbeat())
        }
      }
    }

    class JustHeartbeats extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Heartbeat() => {
          sender.send(node.input, Messages.Heartbeat())
        }
      }
    }

    // acceptor
    class AcceptorStart extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Propose(value, initiator) => {
          node.behaviour = new AcceptorWaitForPreCommit(value)
          sender.send(node.input, Messages.Accept())
        }
      }
    }

    class AcceptorWaitForPreCommit(value:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.PreCommit() => {
          node.behaviour = new AcceptorWaitForCommit(value)
          sender.send(node.input, Messages.PreCommitAck())
        }

        case Messages.Fail() => {
          node.behaviour = new DeadNodeBehaviour()
        }
      }
    }

    class AcceptorWaitForCommit(value:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Commit() => {
          node.behaviour = new DeadNodeBehaviour()
          node.storage.put(value)
        }
      }
    }

    class RejectorStart extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Propose(_, _) => {
          sender.send(node.input, Messages.Reject())
        }

        case Messages.Fail() => {} // transaction failed, how unfortunate...
      }
    }

    // watchdog
    class WatchdogStart extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Propose(_, initiator) => {
          node.behaviour = new WatchdogWatchVote(initiator)
          sender.send(node.input, Messages.Accept())
        }
      }
    }

    class WatchdogWatchVote(initiator:String) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Fail() => {
          // nothing to do, vote failed, this was delivered to the rest of the nodes in case of reliable network
          node.behaviour = new DeadNodeBehaviour()
        }

        case Messages.PreCommit() => {
          node.behaviour = new WatchdogWatchCommit(initiator)
          sender.send(node.input, Messages.PreCommitAck())
        }
      }
    }

    class WatchdogWatchCommit(initiator:String) extends NodeBehaviour {
      var timeWithoutHeartbeat = 0
      val heartbeatInterval = 3
      val heartbeatThreshold = 10
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Heartbeat() => {
          timeWithoutHeartbeat = 0
        }

        case Messages.Commit() => {
          // it's over, we can stop watching
          node.behaviour = new DeadNodeBehaviour()
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        timeWithoutHeartbeat += 1
        if (timeWithoutHeartbeat > heartbeatInterval) {
          node.cluster.get.nodes(initiator).input.send(node.input, Messages.Heartbeat())
        }

        if (timeWithoutHeartbeat > heartbeatThreshold) {
          node.cluster.get.multicastExceptMe(node, Messages.Commit())
        }
      }
    }

  }
}
  

