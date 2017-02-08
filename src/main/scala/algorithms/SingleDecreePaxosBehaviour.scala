package algorithms

import channel.{Channel, Message}
import cluster.{DeadNodeBehaviour, Node, NodeBehaviour}

object SingleDecreePaxosBehaviour {
  object Messages {
    case class Propose(n:Int, value:String) extends Message
    case class Promise(n:Int, prevValue:Option[String]) extends Message
    case class Accept(value:String, n:Int) extends Message
    case class Accepted(n:Int) extends Message
  }

  // this is a cheat, we use the same number generator across all nodes
  // literally the same object
  // in real system one can use node_id and then pack 64bit intereger as following:
  // first 32 bits are i that increases
  // second 32bits are node_id
  // this will give an unique numbers across nodes, we need only to keep numbers unique at single node now
  // and also possibility to generate a value greater than received last time as i goes first
  class ProposalNumberGenerator {
    private var biggestSoFar = 0
    def greaterThan(x:Int) = {
      biggestSoFar = x + 1 max biggestSoFar
      biggestSoFar
    }
  }


  object Behaviors {
    class Proposer(acceptors:Set[Node.NodeId],
                   value:String,
                   generator:ProposalNumberGenerator,
                   lastIssuedNumber:Int = 0) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = ???

      override def tick(time:Int, node:Node) = {
        val issuedNumber = generator.greaterThan(lastIssuedNumber)
        acceptors.foreach(a => node.cluster.get.nodes(a).input.send(node.input, Messages.Propose(issuedNumber, value)))
        node.behaviour = new ProposerWaitForPromises(acceptors, value, generator, issuedNumber)
        node.logWithTime(time, s"issued proposal with number ${issuedNumber} and value ${value}")
      }
    }

    private class ProposerWaitForPromises(acceptors:Set[Node.NodeId],
                                  private var value:String,
                                  generator: ProposalNumberGenerator,
                                  issuedNumber:Int) extends NodeBehaviour {
      private var promisesToGather = acceptors.size / 2 + 1
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit =
        msg match {
          case Messages.Promise(n, prevValue) =>
            if (promisesToGather > 0) {
              if (issuedNumber == n) {
                promisesToGather -= 1
                node.logWithTime(time, s"${promisesToGather} promises left for proposal ${issuedNumber}")
              } else if (issuedNumber < n) {
                value = prevValue.filter(v => v.compareTo(value) < 0).orElse(Some(value)).get
                node.behaviour = new Proposer(acceptors, value, generator, n)
              }
            }
          case _:Messages.Accepted => node.behaviour = new DeadNodeBehaviour() // we are done here
        }

      override def tick(time: Int, node: Node): Unit =
        if (promisesToGather <= 0) {
          acceptors.foreach(a => node.cluster.get.nodes(a).input.send(node.input, Messages.Accept(value, issuedNumber)))
        }
    }

    class Acceptor extends NodeBehaviour {
      private var _promisedMin = Option.empty[Int]
      private var value = Option.empty[String]

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit =
        msg match {
          case req:Messages.Propose =>
            if (_promisedMin.isEmpty || _promisedMin.get < req.n) {
              node.logWithTime(time, s"promising for proposal ${req.n} with value ${req.value}")
              _promisedMin = Some(req.n)
              sender.send(node.input, Messages.Promise(req.n, value))
            } else {
              node.logWithTime(time, s"rejecting proposal ${req}, issuing better one with n = ${_promisedMin.get} and value ${value}")
              sender.send(node.input, Messages.Promise(_promisedMin.get, value))
            }

          case req:Messages.Accept =>
            if (_promisedMin.isEmpty || _promisedMin.get <= req.n) {
              node.logWithTime(time, s"accepting ${_promisedMin.get} with value ${value}")
              value = Some(req.value)
              node.storage.put(req.value)
              sender.send(node.input, Messages.Accepted(req.n))
            }
      }
    }
  }

}
