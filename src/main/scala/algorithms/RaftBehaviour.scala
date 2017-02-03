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
                       matchIndex:Int,
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
      val leaderElection = 70
      val leaderElectionRandomization = 25
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
          node.log(s"AppendEntries false req.term is ${req.term}, our term is ${state.currentTerm}, prev entry term ${prevEntry}, req.prevLogTerm is ${req.prevLogTerm}, storage is ${node.storage.asList.map(_.asInstanceOf[RaftLogEntry])}")
          sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm, 0, success = false))
          false
        } else {
          // 3. if an existing entry conflicts with a new (same index, different terms)
          for {ours <- node.storage.get(req.prevLogIndex + 1).collect({case a:RaftLogEntry => a})} {
            if (ours.term != req.term) { // TODO: here
              node.log(s"detected log inconsistency ours term ${ours} != theirs ${req.term}, shrinking storage to leader commit ${req.leaderCommit}")
              node.storage.shrinkRight(req.leaderCommit + 1)
              node.log(s"storage now is ${node.storage.asList.map(_.asInstanceOf[RaftLogEntry])}")
              sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm, 0, success = false))
              return false
            }
          }
          // 4. Append new entries
          req.entries.foreach(le => node.storage.put(le))
          // 5. If leader commit > commitIndex
          if (req.leaderCommit > state.commitIndex)
            state.commitIndex = Math.min(req.leaderCommit, state.commitIndex)
          sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm,
            node.storage.size - 1, success = true))
          node.log(s"AppendEntries ${req.entries} true, now storage size is ${node.storage.size}")
          true
        }
      }

      def handleVote(sender:Channel, node:Node, state:CommonState, req:Messages.Vote.Request) = {
        val lastEntry = node.storage.get(req.lastLogIndex).map(_.asInstanceOf[RaftLogEntry])
        if (req.term < state.currentTerm ||
          (state.votedFor.isDefined && state.votedFor.get != req.candidateId) ||
          lastEntry.isEmpty || lastEntry.get.term != req.lastLogTerm) {
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
      var lastLeader:Option[Node.NodeId] = None


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
            lastLeader = Some(req.leaderId)
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
          if (handleVote(sender, node, state, req))
            leaderHeartbeating.voted(time)
        }

        case req:Messages.AppendEntries.Reply =>
          if (state.currentTerm < req.term) {
            state.votedFor = None
            req.term.max(state.currentTerm)
          }

        case req:Messages.Vote.Reply =>
          if (state.currentTerm < req.term) {
            state.votedFor = None
            req.term.max(state.currentTerm)
          }

        case req:Messages.ClientPut.Request => {
          // forward to leader if there are one
          if (lastLeader.isDefined) {
            node.cluster.get.nodes(lastLeader.get).input.send(node.input, req)
          } else {
            sender.send(node.input, Messages.ClientPut.Reply(false))
          }
        }
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
        case Messages.Vote.Reply(term, isVoteGranted) =>
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
            node.logWithTime(time, s"becoming a leader with storage ${node.storage.asList.map(_.asInstanceOf[RaftLogEntry])}")
            node.behaviour = new Leader(state, raftNodes)
          }

        case req:Messages.AppendEntries.Request =>
          if (req.term >= state.currentTerm) {
            handleAppendEntries(sender, node, req)
            state.currentTerm = req.term
            node.behaviour = new Follower(raftNodes, state)
          }

        case req:Messages.Vote.Request =>
          if (handleVote(sender, node, state, req)) {
            state.currentTerm = req.term.max(state.currentTerm)
            node.behaviour = new Follower(raftNodes, state)
          }
          leaderHeartbeating.voted(time)

        case _:Messages.ClientPut.Request =>
          sender.send(node.input, Messages.ClientPut.Reply(success = false))
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
          node.logWithTime(time, s"client put value ${req.value}, going to append it to followers at index ${node.storage.size}, current commit index is ${state.commitIndex}")
          node.storage.put(RaftLogEntry(req.value, state.currentTerm))
          clientRequests = ClientRequest(node.storage.size - 1, sender)::clientRequests
          matchIndex = matchIndex.updated(node.nodeId, matchIndex.getOrElse(node.nodeId, 0) + 1)
          nextIndex = nextIndex.updated(node.nodeId, nextIndex(node.nodeId) + 1)
        }

        case req:Messages.AppendEntries.Reply =>
          if (req.term <= state.currentTerm) {
            if (req.success) { // else it was due to log inconsistancy
              // update match index and nextIndex
              matchIndex = matchIndex.updated(req.senderId, req.matchIndex)
              nextIndex = nextIndex.updated(req.senderId, (nextIndex(req.senderId) + 1)
                .min(matchIndex(req.senderId) + 1).min(node.storage.size))
              node.logWithTime(time, s"match index is ${matchIndex}")
              node.logWithTime(time, s"next index is ${nextIndex}")
              // update state.commitIndex
              if (state.commitIndex != node.storage.size - 1) {
                node.log(s"updating ${state.commitIndex}, as not equal to last entry ${node.storage.size - 1}")
                val oldCommitIndex = state.commitIndex
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
                if (oldCommitIndex != state.commitIndex)
                  node.logWithTime(time, s"updated commit index from ${oldCommitIndex} to ${state.commitIndex}")
                // send replies to requests
                val (completedRequests, newClientRequests) = clientRequests.partition(r => r.logIdx <= state.commitIndex)
                clientRequests = newClientRequests
                completedRequests.foreach(r => r.callback.send(node.input, Messages.ClientPut.Reply(true)))
                node.logWithTime(time, s"completing requests ${completedRequests}")
              }
            } else {
              node.logWithTime(time, s"resync node ${req.senderId} as its log was inconsistent at index ${nextIndex(req.senderId)}")
              nextIndex = nextIndex.updated(req.senderId, nextIndex(req.senderId) - 1)
            }
          } else { // we are not longer leader, as there are higher term discovered
            node.logWithTime(time, s"no longer a leader, higher term discovered, ${state.currentTerm} > ${req.term}")
            node.behaviour = new Follower(raftNodes, state)
          }

        case req:Messages.Vote.Request => {
            if (handleVote(sender, node, state, req)) {
              state.currentTerm = state.currentTerm.max(req.term)
              node.behaviour = new Follower(raftNodes, state)
            }
        }

        case req:Messages.AppendEntries.Request => {
          if (state.currentTerm < req.term) {
            state.currentTerm = req.term
            state.votedFor = None
            node.behaviour = new Follower(raftNodes, state)
            handleAppendEntries(sender, node, req)
          } else sender.send(node.input, Messages.AppendEntries.Reply(node.nodeId, state.currentTerm, 0, success = false))
        }
      }

      override def tick(time: Int, node: Node): Unit = {
        if (lastHeartbeatSent.isEmpty) {
          lastHeartbeatSent = Some(time)
        }

        val lastEntryIndex = node.storage.size - 1
        if (nextIndex.isEmpty)
          nextIndex = raftNodes.map(n => n -> nextIndex.getOrElse(n, lastEntryIndex + 1)).toMap

        if (time - lastHeartbeatSent.get > Timeouts.heartbeating) {
          lastHeartbeatSent = Some(time)
          raftNodes.filterNot(_ == node.nodeId).foreach(
            targetNode => {
              node.logWithTime(time, s"syncing node ${targetNode} with index ${nextIndex(targetNode)} and storage ${node.storage.asList.map(_.asInstanceOf[RaftLogEntry])}")
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
  }

}
