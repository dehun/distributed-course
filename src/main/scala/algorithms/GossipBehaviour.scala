package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}


object GossipBehaviour  {
  object Messages {
    object Gossip {
      var instantiations:Int = 0
    }
    case class Gossip(nodes:Set[String], sender:String) extends Message {
      Gossip.instantiations += 1
    }
  }


  class GossipBehaviour(var initialNodes:Set[String]) extends NodeBehaviour {
    var nodesToGossipOnNextTick = initialNodes // when we start we want to gossip about our startup nodes
    var knowledge:Map[String, Set[String]] = Map()

    override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
      case Messages.Gossip(newNodes, sender) => {
        nodesToGossipOnNextTick ++= newNodes
        knowledge = knowledge.updated(sender, knowledge.getOrElse(sender, Set()) ++ newNodes)
      }
    }

    override def tick(time: Int, node: Node): Unit = {
      knowledge = knowledge.updated(node.nodeId, knowledge.getOrElse(node.nodeId, Set()) ++ nodesToGossipOnNextTick)
      knowledge(node.nodeId).foreach(
        name => node.cluster.get.nodes.get(name).foreach(
          n => {
            val nodesToSend = nodesToGossipOnNextTick -- knowledge.getOrElse(name, Set()) -- Set(name)
            if (!nodesToSend.isEmpty) {
              n.input.send(node.input, Messages.Gossip(nodesToSend, node.nodeId))
            }
          }))
      nodesToGossipOnNextTick = Set.empty
    }
  }

  class ReliableGossipBehaviour(val initialNodes:Set[String]) extends NodeBehaviour {
    var knowledge:Map[String, Set[String]] = Map()

    override def init(node: Node): Unit = {
      knowledge = knowledge.updated(node.nodeId, initialNodes)
    }

    // on every tick we are going to replicate to all known nodes the nodes unknown to particular node
    // we are considering nodes to be known to other node only if node send it to us

    override def onMessage(sender: Channel, msg: Message, node: Node): Unit = msg match {
      case Messages.Gossip(nodes, sender) => {
        knowledge = knowledge.updated(node.nodeId, knowledge.getOrElse(node.nodeId, Set()) ++ nodes + sender)
        knowledge = knowledge.updated(sender, knowledge.getOrElse(sender, Set()) ++ nodes + node.nodeId)
      }
    }

    override def tick(time: Int, node: Node): Unit = {
      knowledge(node.nodeId).foreach(otherNodeName => {
        val ourKnowledge = knowledge(node.nodeId)
        val otherKnowledge = knowledge.getOrElse(otherNodeName, Set())
        val knowledgeToSend = ourKnowledge -- otherKnowledge
        if (!knowledgeToSend.isEmpty) {
          node.cluster.get.nodes(otherNodeName).input.send(node.input, Messages.Gossip(knowledgeToSend, node.nodeId))
        }
      })
    }
  }
}
