package algorithms

import channel.{Channel, Message}
import cluster.{DeadNodeBehaviour, Node, NodeBehaviour}

object TriplePhaseCommitBehaviour {
  object Messages {
    case class Propose(value: Int) extends Message
    case class Accept() extends Message
    case class Reject() extends Message
    case class PreCommit() extends Message
    case class PreCommitAck() extends Message
    case class Commit() extends Message
    case class Fail() extends Message
  }

  object Behaviours {
    // proposer
    class ProposerStart(proposal:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = ???
      override def tick(time: Int, node: Node): Unit = {
        node.behaviour = new ProposerWaitForAccepts(proposal, node.cluster.get.nodes.size - 1)
        node.cluster.get.multicastExceptMe(node, new Messages.Propose(proposal))
      }
    }

    class ProposerWaitForAccepts(value:Int, acksToPreCommit:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
        case Messages.Accept() => {
          if (acksToPreCommit > 1) {
            node.behaviour = new ProposerWaitForAccepts(value, acksToPreCommit - 1)
          } else {
            node.behaviour = new ProposerWaitForPreCommitAcks(value, node.cluster.get.nodes.size - 1)
            node.cluster.get.multicastExceptMe(node, Messages.PreCommit())
          }
        }

        case Messages.Reject() => {
          node.behaviour = new DeadNodeBehaviour()
          node.cluster.get.multicastExceptMe(node, Messages.Fail())
        }
      }
    }

    class ProposerWaitForPreCommitAcks(value:Int, acksToCommit:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
        case Messages.PreCommitAck() => {
          if (acksToCommit > 1) {
            node.behaviour = new ProposerWaitForPreCommitAcks(value, acksToCommit - 1)
          } else {
            node.behaviour = new DeadNodeBehaviour()
            node.cluster.get.multicastExceptMe(node, Messages.Commit())
            node.storage.put(value)
          }
        }
      }
    }

    // acceptor
    class AcceptorStart extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
        case Messages.Propose(value) => {
          node.behaviour = new AcceptorWaitForPreCommit(value)
          sender.send(node.input, Messages.Accept())
        }
      }
    }

    class AcceptorWaitForPreCommit(value:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
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
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
        case Messages.Commit() => {
          node.behaviour = new DeadNodeBehaviour()
          node.storage.put(value)
        }
      }
    }

    class RejectorStart extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
        case Messages.Propose(_) => {
          sender.send(node.input, Messages.Reject())
        }

        case Messages.Fail() => {} // transaction failed, how unfortunate...
      }
    }

  }
}
  

