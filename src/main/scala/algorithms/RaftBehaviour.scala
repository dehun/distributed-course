package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}

import scala.util.Random

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
    object ClientPut {
      case class Request(value:String) extends Message
      case class Reply(success:Boolean) extends Message
    }
  }

  case class RaftLogEntry(value:String, term:Int)

  object Behaviours {
    object Timeouts {
      val leaderElection = 50
      val leaderElectionRandomization = 10
    }

    class CommonState {
      def increaseTerm() = currentTerm += 1

      var currentTerm:Int = 0
      var votedFor:Option[Node.NodeId] = None

      var commitIndex:Int = 0
      var lastApplied:Int = 0
    }


    class LeaderHeartbeating(offset:Int) {
      var lastSawLeaderOrVoted:Option[Int] = None

      def sawLeader(time:Int):Unit = lastSawLeaderOrVoted = Some(time)

      def voted(time:Int):Unit = lastSawLeaderOrVoted = Some(time)

      def tryToBecameCandidate(time:Int):Boolean = {
        if (lastSawLeaderOrVoted.isDefined &&
          time - lastSawLeaderOrVoted.get > Timeouts.leaderElection) {
          true
        } else {
          if (lastSawLeaderOrVoted.isEmpty)
            lastSawLeaderOrVoted = Some(time + offset)
        }
        false
      }
    }

    class Follower(raftNodes:Set[Node.NodeId], state:CommonState = new CommonState()) extends NodeBehaviour {
      val leaderHeartbeating = new LeaderHeartbeating(Random.nextInt(Timeouts.leaderElectionRandomization))
      val lastLeader:Option[Node.NodeId] = None

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match  {
        case req:Messages.AppendEntries.Request => {
          // 3. if an existing entry conflicts with a new (same index, different terms)
          for {ours <- node.storage.get(req.leaderCommit).collect({case a:RaftLogEntry => a})} {
            if (ours.term != req.term) {
              node.storage.shrinkRight(req.leaderCommit)
            }
          }
          // 1. if term < currentTerm reply false
          // 2. if prevLogIndex with requested term does not exists reply false
          val prevEntry = node.storage.get(req.prevLogIndex)
          if (req.term < state.currentTerm ||
            prevEntry.isEmpty ||
            prevEntry.get.asInstanceOf[RaftLogEntry].term != req.prevLogTerm
           ) {
            sender.send(node.input, Messages.AppendEntries.Reply(state.currentTerm, false))
          } else {

          }
        }

        case req:Messages.Vote.Request => {
          if (req.term < state.currentTerm ||
            (state.votedFor.isDefined && state.votedFor.get != req.candidateId)
          ) {
            sender.send(node.input, Messages.Vote.Reply(state.currentTerm, false))
          } else {
            sender.send(node.input, Messages.Vote.Reply(state.currentTerm, true))
            state.votedFor = Some(req.candidateId)
          }
          leaderHeartbeating.voted(time) // TODO: should be here, or only when we vote true?
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        // check that we still have a leader
        if (leaderHeartbeating.tryToBecameCandidate(time)) {
          node.behaviour = new Candidate(state, raftNodes)
        }
      }
    }

    class Candidate(state:CommonState, raftNodes:Set[Node.NodeId]) extends NodeBehaviour {
      private var _tick = (time:Int, node:Node) => _onBecome(time, node)
      private var _repliesToReceive = raftNodes.size / 2 + 1

      private def _onBecome(time:Int, node:Node):Unit = {
        state.increaseTerm()
        val lastEntry = node.storage.asList.last.asInstanceOf[RaftLogEntry]
        val requestVote = Messages.Vote.Request(
          state.currentTerm, node.nodeId,
          node.storage.size - 1, lastEntry.term)
        raftNodes.map(n => node.cluster.get.nodes(n)).foreach(n => n.input.send(node.input, requestVote))
        _tick = (time:Int, node:Node) => _normalTick(time, node)
      }

      private def _normalTick(time:Int, node:Node):Unit = {

      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Vote.Reply(term, isVoteGranted) =>
          if (isVoteGranted) {
            _repliesToReceive -= 1
          } else {
            node.behaviour = new Follower(raftNodes, state)
          }

        case req:Messages.AppendEntries.Request => {
          if (req.term >= state.currentTerm) {
            node.behaviour = new Follower(raftNodes, state)
          }
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        _tick(time, node)
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
