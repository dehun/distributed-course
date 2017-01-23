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
    object Timeouts {
      val leaderElection = 50
    }

    class CommonState {
      var currentTerm:Int = 0
      var votedFor:Option[Node.NodeId] = None

      var commitIndex:Int = 0
      var lastApplied:Int = 0
    }


    trait LeaderHeartbeating {
      var lastSawLeaderOrVoted:Option[Int] = None

      def sawLeader(time:Int):Unit = lastSawLeaderOrVoted = Some(time)

      def voted(time:Int):Unit = lastSawLeaderOrVoted = Some(time)

      def tick(time:Int, node:Node, state:CommonState):Boolean = {
        if (lastSawLeaderOrVoted.isDefined &&
          time - lastSawLeaderOrVoted.get > Timeouts.leaderElection) {
          node.behaviour = new Candidate(state)
          return true
        } else {
          lastSawLeaderOrVoted = Some(time)
        }
        false
      }
    }

    class Follower(state:CommonState = new CommonState()) extends NodeBehaviour {
      val leaderHeartbeating = new LeaderHeartbeating {}

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match  {
        case _ => ???
      }

      override def tick(time: Int, node: Node): Unit = {
        // check that we still have a leader
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
