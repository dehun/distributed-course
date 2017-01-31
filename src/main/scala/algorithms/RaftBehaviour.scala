package algorithms

import channel.{Channel, Message}
import cluster.{Node, NodeBehaviour}

import scala.util.Random

object RaftBehaviour {
  object Messages {
    object AppendEntries {
      case class Request(term:Int,
                         leaderId:Node.NodeId,
                         prevLogIndex:Int,
                         prevLogTerm:Int,
                         entries: List[RaftLogEntry],
                         leaderCommit:Int) extends Message
      case class Reply(senderId:Node.NodeId,
                       term:Int,
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

      def sawLeader(time:Int):Unit = lastSawLeaderOrVoted = Some(time + offset)

      def voted(time:Int):Unit = lastSawLeaderOrVoted = Some(time + offset)

      def tryToBecameCandidate(time:Int):Boolean = {
        if (lastSawLeaderOrVoted.isDefined &&
          time - lastSawLeaderOrVoted.get > Timeouts.leaderElection) {
          true
        } else {
          if (lastSawLeaderOrVoted.isEmpty) {
            Console.println(s"startng with offset ${offset}")
            lastSawLeaderOrVoted = Some(time + offset)
          }
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
        if (req.term  < state.currentTerm ||
          prevEntry.isEmpty ||
          prevEntry.get.term != req.prevLogTerm
        ) {
          node.log(s"AppendEntries false req.term is ${req.term}, our term is ${state.currentTerm}, prev entry term ${prevEntry}, req.prevLogTerm is ${req.prevLogTerm}")
          sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm, success = false))
          false
        } else {
          // 3. if an existing entry conflicts with a new (same index, different terms)
          for {ours <- node.storage.get(req.leaderCommit).collect({case a:RaftLogEntry => a})} {
            if (ours.term != req.term) {
              node.log(s"detected log inconsistency, shrinking storage to leader commit ${req.leaderCommit}")
              node.storage.shrinkRight(req.leaderCommit + 1)
            }
          }
          // 4. Append new entries
          req.entries.foreach(le => node.storage.put(le))
          // 5. If leader commit > commitIndex
          if (req.leaderCommit > state.commitIndex)
            state.commitIndex = Math.min(req.leaderCommit, state.commitIndex)
          sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm, success = true))
          true
        }
      }

      def handleVote(sender:Channel, node:Node, state:CommonState, req:Messages.Vote.Request) = {
        // TODO: where is req.lastIndex and req.lastTerm checks?
        if (req.term < state.currentTerm ||
          (state.votedFor.isDefined && state.votedFor.get != req.candidateId)
        ) {
          node.log(s"voting no for candidate ${req.candidateId}, req.term is ${req.term}, our term is ${state.currentTerm}")
          sender.send(node.input, Messages.Vote.Reply(state.currentTerm, false))
          false
        } else {
          node.log(s"voting yes for candidate ${req.candidateId}, req.term is ${req.term}, our term is ${state.currentTerm}")
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
        if (node.storage.size == 0) {
          node.storage.put(RaftLogEntry("placeholder", 0))
          state.commitIndex = 0
        }
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match  {
        case req:Messages.AppendEntries.Request => {
          if (handleAppendEntries(sender, node, req)) // TODO: double check this
            leaderHeartbeating.sawLeader(time)
          if (req.term > state.currentTerm) {
            state.currentTerm = req.term
            state.votedFor = None
          }
        }

        case req:Messages.Vote.Request => {
          if (state.currentTerm < req.term) {
            state.currentTerm = req.term
            state.votedFor = None
          }
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
      // WTF we need leader heartbeating here?
      private val leaderHeartbeating = new LeaderHeartbeating(Random.nextInt(Timeouts.leaderElectionRandomization))

      private def _onBecome(time:Int, node:Node):Unit = {
        node.logWithTime(time, "starting election")
        state.increaseTerm()
        val lastEntry = node.storage.asList.last.asInstanceOf[RaftLogEntry]
        val requestVote = Messages.Vote.Request(
          state.currentTerm, node.nodeId,
          node.storage.size - 1, lastEntry.term)
        raftNodes.filter(_ != node.nodeId).map(n => node.cluster.get.nodes(n)).foreach(n => n.input.send(node.input, requestVote))
        _tick = (time:Int, node:Node) => _normalTick(time, node)
        state.votedFor = Some(node.nodeId)
      }

      private def _normalTick(time:Int, node:Node):Unit = {
        if (leaderHeartbeating.tryToBecameCandidate(time)) {
          node.log("term ended without leader, restarting as candidate")
          node.behaviour = new Candidate(state, raftNodes)
        }
      }

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case Messages.Vote.Reply(term, isVoteGranted) => {
          if (isVoteGranted) {
            node.logWithTime(time, s"received yes vote")
            _repliesToReceive -= 1
          } else {
            node.logWithTime(time, s"received no vote, reverting")
            state.votedFor = None
            state.currentTerm = term.max(state.currentTerm)
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
            state.currentTerm = req.term
            node.behaviour = new Follower(raftNodes, state)
          }
        }

        case req:Messages.Vote.Request => {
          if (handleVote(sender, node, state, req)) {
            state.currentTerm = req.term.max(state.currentTerm)
            node.behaviour = new Follower(raftNodes, state)
          }
          leaderHeartbeating.voted(time)
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        _tick(time, node)
      }
    }

    case class ClientRequest(logIdx:Int, callback:Channel)

    class Leader(override val state:CommonState, override val raftNodes:Set[Node.NodeId]) extends NodeBehaviour with RaftBehavior {
      private var lastHeartbeatSent:Option[Int] = None
      private var nextIndex = Map.empty[Node.NodeId, Int]
      private var matchIndex = raftNodes.map(n => n -> 0).toMap
      private var clientRequests = List.empty[ClientRequest]

      override def onMessage(sender: Channel, msg: Message, node: Node, time: Int): Unit = msg match {
        case req:Messages.Vote.Reply =>
          if (!req.voteGranted && req.term > state.currentTerm) {
            node.logWithTime(time, s"leader received vote reply with higher term ${req.term} > its own ${state.currentTerm}, reverting to follower")
            state.currentTerm = req.term
            node.behaviour = new Follower(raftNodes, state)
          }

        case req:Messages.ClientPut.Request => {
          node.logWithTime(time, s"client put value ${req.value}")
          node.storage.put(RaftLogEntry(req.value, state.currentTerm))
          clientRequests = ClientRequest(node.storage.size - 1, sender)::clientRequests
        }

        case req:Messages.AppendEntries.Reply =>
          if (req.term <= state.currentTerm) {
            if (req.success) { // if it was due to log inconsistancy
              // update match index
              matchIndex = matchIndex.updated(req.senderId, nextIndex(req.senderId))
              // update state.commitIndex
              if (state.commitIndex != node.storage.size - 1) {
                val oldCommitIndex = state.commitIndex
                node.log(s"updating ${state.commitIndex}, as not equal to last entry ${node.storage.size - 1}")
                state.commitIndex = (state.commitIndex until node.storage.size).maxBy(
                  i => {
                    val majority = raftNodes.size / 2 + 1
                    val entry = node.storage.get(i).get.asInstanceOf[RaftLogEntry]
                    if ((raftNodes.count(rn => matchIndex(rn) >= i) >= majority) &&
                      (entry.term == state.currentTerm)) {
                      i
                    } else {
                      state.commitIndex
                    }
                  })
                // send replies to requests
                val (completedRequests, newClientRequests) = clientRequests.partition(r => r.logIdx <= state.commitIndex)
                clientRequests = newClientRequests
                completedRequests.foreach(r => r.callback.send(node.input, Messages.ClientPut.Reply(true)))
                node.log(s"comleting requests ${completedRequests}")
              }
              node.logWithTime(time, s"update commit index to ${state.commitIndex}")
            } else {
              node.logWithTime(time, s"resync node ${req.senderId} as its log was inconsistent at index ${nextIndex(req.senderId)}")
              nextIndex = nextIndex.updated(req.senderId, nextIndex(req.senderId) - 1)
            }
          } else { // we are not longer leader, as there are higher term discovered
            node.logWithTime(time, s"no longer a leader, higher term discovered, ${state.currentTerm} > ${req.term}")
            node.behaviour = new Follower(raftNodes, state)
          }

        case req:Messages.Vote.Request => {
          handleVote(sender, node, state, req) // TODO: return value check?
        }

        case req:Messages.AppendEntries.Request => {
          // TODO: implement me
          ???
        }
      }

      def syncNode(node:Node):Unit = {
        // TODO: implement me
      }

      override def tick(time: Int, node: Node): Unit = {
        if (lastHeartbeatSent.isEmpty) {
          lastHeartbeatSent = Some(time)
        }

        val lastEntryIndex = node.storage.size - 1
        nextIndex = raftNodes.map(n => n -> nextIndex.getOrElse(n, lastEntryIndex + 1)).toMap

        if (time - lastHeartbeatSent.get > Timeouts.heartbeating) {
          lastHeartbeatSent = Some(time)
          raftNodes.filterNot(_ == node.nodeId).foreach(
            targetNode => {
              node.logWithTime(time, s"syncing node ${targetNode}")
              val prevIndex = nextIndex(targetNode) - 1
              val prevEntry = node.storage.get(prevIndex).get.asInstanceOf[RaftLogEntry]
              val entries = (prevIndex + 1 until node.storage.size).map(
                i => node.storage.get(i).get.asInstanceOf[RaftLogEntry]) toList
              val heartbeatMsg = Messages.AppendEntries.Request(
                state.currentTerm, node.nodeId, prevIndex,
                prevEntry.term, entries, state.commitIndex)
              node.cluster.get.nodes(targetNode).input.send(node.input, heartbeatMsg)
            })
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
