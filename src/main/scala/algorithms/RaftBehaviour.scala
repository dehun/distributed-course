package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}

// TODO: implement me
object RaftBehaviour {
  object Messages {
    object AppendEntries {
      case class Request(term:Int,
                         leaderId:Node.NodeId,
                         prevLogIndex:Int,
                         prevLogTerm:Int,
                         entries: List[Int],
                         leaderCommit:Int) extends Message
      case class Reply(term:Int,
                       success:Boolean) extends Message

    }
    object Vote {
      case class Request(term:Int,
                         candidateId:Node.NodeId,
                         lastLogIndex:Int,
                         lastLogTerm:Int) extends Message
      case class Reply(term:Int, voteGranted:Boolean) extends Message
    }
  }

  object Behaviours {
    class CommonState {
      var currentTerm:Int = 0
      var votedFor:Option[String] = None

      var commitIndex:Int = 0
      var lastApplied:Int = 0
    }


    class Follower(state:CommonState = new CommonState()) extends NodeBehaviour {

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match  {
        case _ => ???
      }

      override def tick(time: Int, node: Node): Unit = {

      }
    }

    class Candidate(state:CommonState) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _ => ???
      }
    }

    class Leader(state:CommonState) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _ => ???
      }
    }

    class Client(backendNodes:List[Node], private var valuesToStore:Stream[Int]) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _ => ???
      }
    }


  }

}
