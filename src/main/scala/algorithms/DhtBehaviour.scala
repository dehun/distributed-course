package algorithms

import channel.{Channel, Message}
import cluster.{DeadNodeBehaviour, Node, NodeBehaviour}
import com.sun.org.apache.xml.internal.security.algorithms.MessageDigestAlgorithm

import scala.util.Random


object DhtBehaviour extends {
  object Messages {
    case class Store(value:Int) extends Message
    case class Query(value:Int) extends Message
    case class QueryFound(value:Int, node:String) extends Message
    case class QueryNotFound(value:Int) extends Message
  }

  object Behaviours {
    class Client(valuesToStore:Stream[Int], backendNodes:List[Node]) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = {}
      override def tick(time: Int, node: Node): Unit = {
        // feed values to random nodes one by one
        if (valuesToStore.size > 0) {
          val choosenBackend = backendNodes(Random.nextInt(backendNodes.size))
          choosenBackend.input.send(node.input, Messages.Store(valuesToStore.head))
          node.behaviour = new Client(valuesToStore.tail, backendNodes)
        }
      }
    }

    case class QueryResult(value:Int, where:Option[String])

    class QueryingClient(private var valuesToQuery:Stream[Int],
                         backendNodes:List[Node]) extends NodeBehaviour {
      var results:Set[QueryResult] = Set.empty

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.QueryFound(value, where) => {
          results += QueryResult(value, Some(where))
        } case Messages.QueryNotFound(value) => {
          results += QueryResult(value, None)
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        if (!valuesToQuery.isEmpty) {
          val value = valuesToQuery.head
          valuesToQuery = valuesToQuery.tail
          val choosenBackend = backendNodes(Random.nextInt(backendNodes.size))
          choosenBackend.input.send(node.input, Messages.Query(value))
        }
      }
    }

    class BackendNode extends NodeBehaviour{
      var backendNodes:Option[List[Node]] = None

      override def init(node: Node): Unit = {
        backendNodes = Some(node.cluster.get.nodes.values.filter(n => n.behaviour.isInstanceOf[BackendNode]) toList)
      }

      override def tick(time: Int, node: Node): Unit = super.tick(time, node)

      def pickBackendNode(value:Int):Node = {
        backendNodes.get(value % backendNodes.get.size)
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Store(value) => {
          val backendNode = pickBackendNode(value)
          if (node.eq(backendNode)) {
            node.storage.put(value)
          } else {
            backendNode.input.send(node.input, Messages.Store(value))
          }
        }

        case query@Messages.Query(value) => {
          val onNode = pickBackendNode(value)
          if (onNode.eq(node)) { // it should be here!
            node.storage.asList.find(_ == value) match {
              case Some(_) => sender.send(node.input, Messages.QueryFound(value, node.nodeId))
              case None => sender.send(node.input, Messages.QueryNotFound(value))
            }
          } else {
            // it is on other node, forward query there (note sender, it's forwarding)
            onNode.input.send(sender, query)
          }
        }

      }
    }
  }

}
