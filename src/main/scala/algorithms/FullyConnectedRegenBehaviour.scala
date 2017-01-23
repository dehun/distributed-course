package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}

object FullyConnectedRegenBehaviour {
  object Messages {
    case class Heartbeat(senderId:Node.NodeId, suspicious:Set[Node.NodeId]) extends Message
    case class Evict() extends Message
  }

  object Timeouts {
    val eviction = 50
    val suspicious = 15
    val heartbeat = 5
  }

  object Behaviours {
    case class Suspicious(nodeId:Node.NodeId, voteCount:Int, firstOccurance:Int)

    class FullyConnectedRegen(var aliveNodes:Set[Node.NodeId]) extends NodeBehaviour {
      var _suspicious = Map.empty[Node.NodeId, Suspicious]
      var _beats = aliveNodes.map(n => (n -> 0)) toMap
      var _evicted = Set.empty[Node.NodeId]
      var ourLastBeat = 0

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Heartbeat(senderId, suspicious) => {
          if (_evicted.contains(senderId)) return
          suspicious.foreach(sid => {
            val prevVal = _suspicious.getOrElse(sid, Suspicious(sid, 0, time))
            val newVal = Suspicious(prevVal.nodeId, prevVal.voteCount + 1, prevVal.firstOccurance)
            _suspicious = _suspicious.updated(sid, newVal)
          })
          _beats = _beats.updated(senderId, time)
          aliveNodes += senderId
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        // kick dead
        val dead = _suspicious.filter(s => time - s._2.firstOccurance > Timeouts.eviction).map(_._1).toList.sorted
        for (walkingDead <- dead.headOption) {
          aliveNodes -= walkingDead
          _beats -= walkingDead
          _suspicious = Map.empty[Node.NodeId, Suspicious]
          _evicted += walkingDead
          Console.println(s"${node.nodeId} evicted ${walkingDead}, alive noes now are ${aliveNodes}")
        }
        // beat em
        if (time - ourLastBeat > Timeouts.heartbeat) {
          val ourSuspicion: Set[Node.NodeId] = _beats.filter(n => time - n._2 > Timeouts.suspicious).map(_._1).toSet
          if (ourSuspicion.nonEmpty) Console.println(s"${node.nodeId} our suspicion is ${ourSuspicion.toList.sorted}, beats are ${_beats}")
          aliveNodes.foreach(n => node.cluster.get.nodes(n).input.send(
            node.input, Messages.Heartbeat(node.nodeId, ourSuspicion)))
          ourLastBeat = time
        }
      }
    }
  }
}
