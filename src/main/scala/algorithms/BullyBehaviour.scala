package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}


object BullyBehaviour {
  object Messages {
    case class Bully(uid:Int, senderId:Node.NodeId) extends Message
    case class FightBack(uid:Int, senderId:Node.NodeId) extends Message
    case class BeLoser(uid:Int, senderId:Node.NodeId) extends Message
  }

  object Behaviours {
    class Bully(uid:Int, var nodesToBully:Set[Node.NodeId]) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.BeLoser(uid, senderId) => {
          nodesToBully = nodesToBully - senderId
        }
        case Messages.FightBack(uid, senderId) => node.behaviour = new Victim(uid)
        case Messages.Bully(bullyingUid, senderId) =>
          if (uid < bullyingUid) {
            sender.send(node.input, Messages.BeLoser(uid, node.nodeId))
            node.behaviour = new Victim(uid)
          } else {
            sender.send(node.input, Messages.FightBack(uid, node.nodeId))
            nodesToBully = nodesToBully - senderId
          }
      }

      override def tick(time: Int, node: Node): Unit = {
        node.cluster.get.nodes.filter(n => nodesToBully.contains(n._1)).values.foreach((n) => {
          n.input.send(node.input, Messages.Bully(uid, node.nodeId))
        })

      }
    }

    class Victim(uid:Int) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _:Messages.BeLoser => {}
        case _:Messages.FightBack => {}
        case _:Messages.Bully => sender.send(node.input, Messages.BeLoser(uid, node.nodeId))
      }
    }
  }
}
