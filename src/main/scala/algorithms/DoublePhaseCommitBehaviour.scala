package algorithms

import channel.{Channel, Message}
import cluster._

object DoublePhaseCommitBehaviour {

  // Happy path:
  //  Propose ->
  //          <- Accept
  //  Commit  ->
  case class ProposeMessage(value: Int) extends Message
  case class AcceptMessage() extends Message
  case class CommitMessage() extends Message

  // Fail path:
  //  Propose ->
  //         <- Reject
  //         -> Fail
  case class RejectMessage() extends Message
  case class FailMessage() extends Message

  class PeerAcceptBehaviour extends NodeBehaviour {
    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
      case ProposeMessage(value) => {
        Console.println(s"${node.nodeId} accepting proposal ${value}")
        node.behaviour = new PeerWaitForCommitBehaviour(value)
        sender.send(node.input, AcceptMessage())
      }
    }
  }

  class PeerWaitForCommitBehaviour(value: Int) extends NodeBehaviour {
    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
      case CommitMessage() => {
        Console.println(s"${node.nodeId} commiting proposal ${value}")
        node.behaviour = new PeerAcceptBehaviour()
        node.storage.put(value)
      }

      case FailMessage() => {
        Console.println(s"${node.nodeId} received fail for ${value}. Reverting")
        node.behaviour = new PeerAcceptBehaviour()
      }

      case ProposeMessage(newValue) => {
        Console.println(s"${node.nodeId} received another proposal ${newValue} while waiting for commit, rejecting new proposal")
        sender.send(node.input, RejectMessage())
      }
    }
  }

  class PeerRejectBehaviour extends NodeBehaviour {
    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
      case ProposeMessage(value) => {
        Console.println(s"${node.nodeId} rejecting proposal ${value}")
        sender.send(node.input, RejectMessage())
      }
      case FailMessage() => {
        Console.println(s"${node.nodeId} received fail message")
      }
      case _ => ???
    }
  }

  class ProposeStartBehaviour(value: Int) extends NodeBehaviour {
    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = {
      Console.println(s"${node.nodeId} in proposer start state received message ${msg.toString()}. ignoring")
    }

    override def tick(time: Int, node: Node): Unit = {
      Console.println(s"${node.nodeId} proposes ${value}")
      node.behaviour = new ProposeWaitAcceptsBehaviour(node.cluster.get.nodes.size - 1, value)
      node.cluster.get.multicastExceptMe(node, ProposeMessage(value))
    }
  }

  class ProposeWaitAcceptsBehaviour(acksToCommit: Int, value:Int) extends NodeBehaviour {
    override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
      case AcceptMessage() => {
        if (acksToCommit > 1) {
          Console.println(s"${node.nodeId} received accept, ${acksToCommit - 1} to go")
          node.behaviour = new ProposeWaitAcceptsBehaviour(acksToCommit - 1, value)
        } else {
          Console.println(s"${node.nodeId} received final accept, sending commits")
          node.behaviour = new DeadNodeBehaviour()
          node.storage.put(value)
          node.cluster.get.multicastExceptMe(node, CommitMessage())
        }
      }

      case RejectMessage() => {
        Console.println(s"${node.nodeId} received fail messages. failing commit for everybody!")
        node.behaviour = new DeadNodeBehaviour()
        node.cluster.get.multicastExceptMe(node, FailMessage())
      }

      case ProposeMessage(newValue) => {
        // reject as we are already waiting for our request to be completed
        Console.println(s"${node.nodeId} received proposal ${newValue} while in ProposeWaitAcceptsBehaviour")
        sender.send(node.input, RejectMessage())
      }

      case FailMessage() => { // sme other transaction failed, no reaction from us
      }
    }
  }
}
