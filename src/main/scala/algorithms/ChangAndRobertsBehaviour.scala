package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}


object ChangAndRobertsBehaviour {
  object Messages {
    case class Elect(ownerId:Node.NodeId, uid:Int) extends Message
  }

   object Behaviour {
     class NonParticipantInitiator(uid:Int, right:Node.NodeId) extends NodeBehaviour {
       // initiator becomes participant immediately, so we can safely omit onMessage
       override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = ???

       override def tick(time: Int, node: Node): Unit = {
         Console.println(s"${node.nodeId} sends ${uid}")
         node.behaviour = new Participant(uid, right)
         node.cluster.get.nodes(right).input.send(node.input, Messages.Elect(node.nodeId, uid))
       }
     }

     class NonParticipant(uid:Int, right:Node.NodeId) extends NodeBehaviour {
       override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
         case Messages.Elect(ownerId, bestUid) => {
           val rightNode = node.cluster.get.nodes(right)
           if (bestUid > uid) {
             rightNode.input.send(node.input, msg)
           }
           else {
             rightNode.input.send(node.input, Messages.Elect(node.nodeId, uid))
           }
           node.behaviour = new Participant(uid, right)
         }
       }
     }

     class Participant(uid:Int, right:Node.NodeId) extends NodeBehaviour {
       override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
         case Messages.Elect(ownerId, bestUid) => {
           if (bestUid == uid) node.behaviour = {
             Console.println(s"${node.nodeId} received uid ${bestUid}, becomes a leader with ${uid}")
             new Leader(uid, right)
           }
           else if (bestUid < uid) {
             Console.println(s"${node.nodeId} received uid ${bestUid}, ignoring as it is less than ${uid}")
           } // do nothing as we already send better candidate
           else if (bestUid > uid) {
             Console.println(s"${node.nodeId} received uid ${bestUid}, forwarding to ${right}")
             node.cluster.get.nodes(right).input.send(node.input, msg)
           }
         }
       }
     }

     class Leader(val uid:Int, right:Node.NodeId) extends NodeBehaviour {
       override def onMessage(sender: Channel, msg: Message, node: Node, time: Int) = {}
     }
  }
}
