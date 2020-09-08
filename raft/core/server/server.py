import time
import threading
import random
from core.states import Leader, Follower, Candidate
from .storage import RAMStorage

from .service_pb2_grpc import RaftServiceServicer, RaftServiceStub
from .service_pb2 import AppendEntriesReply, RequestVoteReply, AppendEntriesRequest, RequestVoteRequest, ClientWriteReply
import grpc

BASE_TIMEOUT_THRESHOLD = 8
SLEEP_INTERVAL = 3


class RPCServer(RaftServiceServicer):
    def __init__(self, identity, peers):
        self.identity = identity
        self._state = Follower(identity)
        self.peers = peers
        self.store = RAMStorage()

        self._last_call_received_ts = time.time()
        self._next_randomize_election_to = self.randomized_timeout()

        self.start_daemon_thread(target=self._timer, args=())

    @staticmethod
    def start_daemon_thread(target, args):
        th = threading.Thread(target=target, args=args)
        th.setDaemon(True)
        th.start()

    @staticmethod
    def randomized_timeout():
        return BASE_TIMEOUT_THRESHOLD + random.randint(0, int(0.5 * BASE_TIMEOUT_THRESHOLD))

    def _timer(self):
        while True:
            time.sleep(SLEEP_INTERVAL)
            now = time.time()
            print("{} logs: {}".format(self._state, self.store.logs))
            if not type(self._state) is Leader:
                if now - self._last_call_received_ts > self._next_randomize_election_to:
                    print("{} timeout".format(self._state))
                    self._next_randomize_election_to = self.randomized_timeout()
                    self._last_call_received_ts = now
                    self._state = Candidate(self.identity,
                                            self._state.current_term,
                                            self._state.committed_index,
                                            self._state.last_applied).routine(data=None)

                    if type(self._state) is Candidate:
                        if self._state.action(self.peers,
                                              self.store.last_log_index,
                                              self.store.last_log_term,
                                              self.send_request_vote):

                            self._state = Leader(self.identity,
                                                 self.peers,
                                                 self.store.last_log_index,
                                                 self._state.current_term,
                                                 self._state.committed_index,
                                                 self._state.last_applied)

                print("{} timer deadline: {}".format(
                    self._state,
                    self._last_call_received_ts + self._next_randomize_election_to - time.time()))

                if type(self._state) is Follower:
                    self._state.routine(data=None)
                    print("{} term: {} committed idx: {}, last applied idx: {}".format(
                        self._state,
                        self._state.current_term,
                        self._state.committed_index,
                        self._state.last_applied,
                    ))
            else:
                self._state.routine(data=None)
                self._state.action(self.peers,
                                   self.store.last_log_index,
                                   self.store.get_log_term_by_index,
                                   self.store.get_entries_from_index,
                                   self.send_append_entries)

                print("{} committed idx: {}, last applied idx:{}, last log idx: {}".format(
                    self._state,
                    self._state.committed_index,
                    self._state.last_applied,
                    self.store.last_log_index)
                )

    def send_append_entries(self, peer, data):
        # only Leader could invoke AppendEntries
        if not type(self._state) is Leader:
            raise TypeError("Only Leader could invoke AppendEntries RPC")
        channel = grpc.insecure_channel(peer)
        stub = RaftServiceStub(channel)

        print("{} send AppendEntries to {}".format(self._state, peer))

        try:
            response = stub.AppendEntries(AppendEntriesRequest(
                term=data["term"],
                leaderId=data["leaderId"],
                prevLogIndex=data["prevLogIndex"],
                prevLogTerm=data["prevLogTerm"],
                entries=data["entries"],
                leaderCommit=data["leaderCommit"]
            ))
        except Exception as e:
            print("{} invoke AppendEntries err: {}".format(self.identity, str(e)))
        else:
            if response.success:
                # 1. If successful: update nextIndex and matchIndex for follower (§5.3)
                if self._state.next_index[peer] <= self.store.last_log_index:
                    self._state.match_index[peer] = self._state.next_index[peer]
                    self._state.next_index[peer] += 1
            else:
                # 2. If AppendEntries fails because of log inconsistency:
                # decrement nextIndex and retry (§5.3)
                self._state.next_index[peer] -= 1

    def send_request_vote(self, peer, data):
        # only Leader could invoke RequestVote
        if not type(self._state) is Candidate:
            raise TypeError("Only Candidate could invoke AppendEntries RPC")

        print("{} send RequestVote to {}".format(self._state, peer))
        channel = grpc.insecure_channel(peer)
        stub = RaftServiceStub(channel)
        try:
            response = stub.RequestVote(RequestVoteRequest(
                term=data["term"],
                candidateId=data["candidateId"]
            ))

        except Exception as e:
            print("{} invoke AppendEntries err: {}".format(self.identity, str(e)))
            return RequestVoteReply(term=-1, voteGranted=False)

        else:
            return response

    def AppendEntries(self, request, context):
        self._last_call_received_ts = time.time()
        if type(self._state) is Follower:
            print("{} receive AppendEntries, prevLogIndex: {}, prevLogTerm:{}, entries: {}".format(
                self._state,
                request.prevLogIndex,
                request.prevLogTerm,
                request.entries
            ))
            # Reply false if term < currentTerm(§5.1)
            if request.term >= self._state.current_term:
                self._next_randomize_election_to = self.randomized_timeout()
                self._state.current_term = request.term
            else:
                return AppendEntriesReply(term=self._state.current_term, success=False)

            if len(request.entries) != 0:
                if request.prevLogTerm != self.store.get_log_term_by_index(request.prevLogIndex):
                    self.store.delete_entry_to_index_exclusively(request.prevLogIndex)
                    return AppendEntriesReply(term=self._state.current_term, success=False)
                else:
                    # 1. If two entries in different logs have the same index and term,
                    # then they store the same command.
                    # 2. If two entries in different logs have the same index and term,
                    # then the logs are identical in all preceding entries.
                    self.store.store_entries_from_index(request.prevLogIndex, self._state.current_term, request.entries)

            if request.leaderCommit > self._state.committed_index:
                commit_index = min(request.leaderCommit, self.store.last_log_index)
                self._state.committed_index = commit_index
            else:
                print("{} receive heartbeat".format(self.identity))

            return AppendEntriesReply(term=self._state.current_term, success=True)

        elif type(self._state) is Candidate:
            # If AppendEntries RPC received from new leader: convert to follower
            self._state = Follower(
                self.identity,
                self._state.current_term,
                self._state.committed_index,
                self._state.last_applied
            )

        return AppendEntriesReply(term=self._state.current_term, success=False)

    def RequestVote(self, request, context):
        self._last_call_received_ts = time.time()
        if type(self._state) is Follower:
            # Reply false if term < currentTerm (§5.1)
            if request.term < self._state.current_term:
                print("{} reject vote: current term_{} is larger than term_{}".format(
                    self._state,
                    self._state.current_term,
                    request.term))

                return RequestVoteReply(term=self._state.current_term, voteGranted=False)

            # If votedFor is null or candidateId, and candidate’s log is at
            # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
            if self._state.voted_for is None or \
                    self._state.current_term == request.term and self._state.voted_for == request.candidateId or \
                    request.term > self._state.current_term:

                # If RPC request or response contains term T > currentTerm:
                # set currentTerm = T, convert to follower (§5.1)
                self._state.current_term = request.term
                if request.lastLogTerm > self.store.last_log_term or \
                        request.lastLogTerm == self.store.last_log_term and \
                        request.lastLogIndex >= self.store.last_log_index:

                    self._state.voted_for = request.candidateId
                    return RequestVoteReply(term=self._state.current_term, voteGranted=True)

        self._state.voted_for = None
        print("{} reject vote: current term_{}_index{} is later than term_{}_index_{}".format(
            self._state,
            self.store.last_log_term,
            self.store.last_log_index,
            request.lastLogTerm,
            request.lastLogIndex,
            request.term))

        return RequestVoteReply(term=self._state.current_term, voteGranted=False)

    def ClientWrite(self, request, context):
        if type(self._state) is Leader:
            self.store.store_entries_from_index(
                self.store.last_log_index,
                self._state.current_term,
                ["{}$#${}".format(request.key, request.value)]
            )

            return ClientWriteReply(success=True)
        else:
            return ClientWriteReply(success=False)
