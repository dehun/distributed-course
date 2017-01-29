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
                         entries: List[String],
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
      val leaderElection = 100
      val leaderElectionRandomization = 20
      val heartbeating = 5
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
          return true
        } else {
          if (lastSawLeaderOrVoted.isEmpty)
            lastSawLeaderOrVoted = Some(time + offset)
          false
        }
      }
    }

    trait RaftBehavior {
      val raftNodes:Set[Node.NodeId]
      val state:CommonState

      def handleAppendEntries(sender:Channel, node:Node, req:Messages.AppendEntries.Request):Boolean = {
        // 1. if term < currentTerm reply false
        // 2. if prevLogIndex with requested term does not exists reply false
        val prevEntry = node.storage.get(req.prevLogIndex).map(_.asInstanceOf[RaftLogEntry])
        if (req.term >= state.currentTerm ||
          prevEntry.isEmpty ||
          prevEntry.get.term != req.prevLogTerm
        ) {
          node.log(s"AppendEntries false req.term is ${req.term}, our term is ${state.currentTerm}, prev entry term ${prevEntry}, req.prevLogTerm is ${req.prevLogTerm}")
          sender.send(node.input, Messages.AppendEntries.Reply(state.currentTerm, success = false))
          false
        } else {
          // 3. if an existing entry conflicts with a new (same index, different terms)
          for {ours <- node.storage.get(req.leaderCommit).collect({case a:RaftLogEntry => a})} {
            if (ours.term != req.term) {
              node.storage.shrinkRight(req.leaderCommit)
            }
          }
          // 4. Append new entries
          req.entries.foreach(v => node.storage.put(RaftLogEntry(v, req.term)))
          // 5. If leader commit > commitIndex
          if (req.leaderCommit > state.commitIndex)
            state.commitIndex = Math.min(req.leaderCommit, node.storage.size - 1)
          true
        }
      }

      def handleVote(sender:Channel, node:Node, state:CommonState, req:Messages.Vote.Request) = {
        if (req.term < state.currentTerm ||
          (state.votedFor.isDefined && state.votedFor.get != req.candidateId)
        ) {
          sender.send(node.input, Messages.Vote.Reply(state.currentTerm, false))
          false
        } else {
          sender.send(node.input, Messages.Vote.Reply(state.currentTerm, true))
          state.votedFor = Some(req.candidateId)
          true
        }
      }
    }

    class Follower(val raftNodes:Set[Node.NodeId],
                   val state:CommonState = new CommonState()) extends NodeBehaviour with RaftBehavior {
      val leaderHeartbeating = new LeaderHeartbeating(Random.nextInt(Timeouts.leaderElectionRandomization))
      val lastLeader:Option[Node.NodeId] = None


      override def init(node: Node): Unit = {
        // we need this as we require previous entry to be present everywhere
        // to avoid additional checks lets have the same entry on all nodes right from the startup
        if (node.storage.size == 0)
          node.storage.put(RaftLogEntry("placeholder", 0))
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match  {
        case req:Messages.AppendEntries.Request =>
          if (handleAppendEntries(sender, node, req)) // TODO: double check this
            leaderHeartbeating.sawLeader(time)

        case req:Messages.Vote.Request => {
          handleVote(sender, node, state, req)
          leaderHeartbeating.voted(time) // TODO: should be here, or only when we vote true?
        }

        case _:Messages.AppendEntries.Reply => {} // TODO: double check
        case _:Messages.Vote.Reply => {}          // TODO: double check
      }

      override def tick(time: Int, node: Node): Unit = {
        // check that we still have a leader
        if (leaderHeartbeating.tryToBecameCandidate(time)) {
          node.logWithTime(time, "becoming candidate as leader was not found ")
          node.behaviour = new Candidate(state, raftNodes)
        }
      }
    }

    class Candidate(override val state:CommonState,
                    override  val raftNodes:Set[Node.NodeId]) extends NodeBehaviour with RaftBehavior {
      private var _tick = (time:Int, node:Node) => _onBecome(time, node)
      private var _repliesToReceive = raftNodes.size / 2 + 1 - 1 // self
      private val leaderHeartbeating = new LeaderHeartbeating(Random.nextInt(Timeouts.leaderElectionRandomization))

      private def _onBecome(time:Int, node:Node):Unit = {
        node.logWithTime(time, "starting election")
        state.increaseTerm()
        val lastEntry = node.storage.asList.last.asInstanceOf[RaftLogEntry]
        val requestVote = Messages.Vote.Request(
          state.currentTerm, node.nodeId,
          node.storage.size - 1, lastEntry.term)
        raftNodes.map(n => node.cluster.get.nodes(n)).foreach(n => n.input.send(node.input, requestVote))
        _tick = (time:Int, node:Node) => _normalTick(time, node)
        state.votedFor = Some(node.nodeId)
      }

      private def _normalTick(time:Int, node:Node):Unit = {
        if (leaderHeartbeating.tryToBecameCandidate(time)) {
          node.behaviour = new Candidate(state, raftNodes)
        }
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Vote.Reply(term, isVoteGranted) => {
          if (isVoteGranted) {
            _repliesToReceive -= 1
          } else {
            node.behaviour = new Follower(raftNodes, state)
          }
          if (_repliesToReceive == 0) {
            node.logWithTime(time, s"becoming a leader")
            node.behaviour = new Leader(state, raftNodes)
          }
        }

        case req:Messages.AppendEntries.Request => {
          if (req.term >= state.currentTerm) {
            handleAppendEntries(sender, node, req)
            node.behaviour = new Follower(raftNodes, state)
          }
        }

        case req:Messages.Vote.Request => {
          handleVote(sender, node, state, req)
          leaderHeartbeating.voted(time) // TODO: should be here, or only when we vote true?
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        _tick(time, node)
      }
    }

    class Leader(override val state:CommonState, override val raftNodes:Set[Node.NodeId]) extends NodeBehaviour with RaftBehavior {
      var lastHeartbeatSent:Option[Int] = None

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _:Messages.Vote.Reply => {} // we already a leader, do nothing with this one
        case _:Messages.AppendEntries.Reply => {} // TODO: implement me
        case req:Messages.Vote.Request => {
          handleVote(sender, node, state, req)
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        if (lastHeartbeatSent.isEmpty) {
          lastHeartbeatSent = Some(time)
        }
        if (time - lastHeartbeatSent.get > Timeouts.heartbeating) {
          val lastIndex = node.storage.size - 1
          val lastEntry = node.storage.get(lastIndex).get.asInstanceOf[RaftLogEntry]
          val heartbeatMsg = Messages.AppendEntries.Request(state.currentTerm, node.nodeId, lastIndex + 1, lastEntry.term, List.empty[String], lastIndex)
          lastHeartbeatSent = Some(time)
          raftNodes.filterNot(_ == node.nodeId).foreach(
            n => node.cluster.get.nodes(n).input.send(node.input, heartbeatMsg))
        }
      }
    }

    class Client(backendNodes:List[Node], private var valuesToStore:Stream[Int]) extends NodeBehaviour {
      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case _ => ???
      }
    }


  }

}
