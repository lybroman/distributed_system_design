from concurrent.futures import ThreadPoolExecutor
TIMEOUT_TH = 5


class BaseState(object):
    def __init__(self, identity, term=0, committed_index=0, last_applied=0):
        self.identity = identity

        # --------------- Persistent Stats
        # latest term server has seen (initialized to 0 on first boot, increases monotonically)
        self.current_term = term
        # candidateId that received vote in current term (or null if none)
        self.voted_for = None

        # --------------- Volatile Stats
        # index of highest log entry known to be committed (initialized to 0, increases monotonically)
        self.committed_index = committed_index
        # index of highest log entry applied to state machine (initialized to 0, increases monotonically)
        self.last_applied = last_applied

    def routine(self, data):
        # If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        if self.committed_index > self.last_applied:
            self.last_applied = self.committed_index

        # If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        if data and hasattr(data, 'term') and data.term > self.current_term:
            return Follower(data.term, self.committed_index, self.last_applied)

        return self

    def action(self, *args):
        raise NotImplementedError("action need to to be implemented in derived state")


class Leader(BaseState):
    def __str__(self):
        return '[Leader::{}]'.format(self.identity)

    __repr__ = __str__

    def __init__(self, identity, peers, last_log_index=0, term=0, committed_index=0, last_applied=0):
        super().__init__(identity, term, committed_index, last_applied)

        # index of the next log entry to send to that server (initialized to leader last log index + 1)
        self.next_index = {peer: last_log_index + 1 for peer in peers}

        # index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        self.match_index = {peer: 0 for peer in peers}

    def action(self, peers, last_log_index, log_term_getter, entries_getter, append_entries):
        # send heartbeat
        with ThreadPoolExecutor(max_workers=max(len(peers), 1)) as executor:
            # If last log index ≥ nextIndex for a follower:
            # send AppendEntries RPC with log entries starting at nextIndex
            # [Roman]: + 1 for heartbeat request
            futures = [executor.submit(append_entries,
                                       peer,
                                       {
                                           "term": self.current_term,
                                           "leaderId": self.identity,
                                           "prevLogIndex": self.next_index[peer] - 1,
                                           "prevLogTerm": log_term_getter(self.next_index[peer] - 1),
                                           "entries": entries_getter(self.next_index[peer]),
                                           "leaderCommit": self.committed_index
                                       })
                       for peer in peers if self.next_index[peer] <= last_log_index + 1]

            for f in futures:
                f.result()

        # If there exists an N such that N > commitIndex, a majority
        # of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
        while self.committed_index < last_log_index:
            N = self.committed_index + 1
            cnt = 0
            for peer in self.match_index:
                if self.match_index[peer] >= N:
                    cnt += 1
                if cnt >= len(peers) // 2:
                    self.committed_index = N
                    break
            else:
                if len(peers) == 0:
                    self.committed_index = N
                break
        return


class Follower(BaseState):
    def __str__(self):
        return '[Follower::{}]'.format(self.identity)

    __repr__ = __str__

    def __init__(self, identity, term=0, committed_index=0, last_applied=0):
        super().__init__(identity, term, committed_index, last_applied)

    def routine(self, data):
        super().routine(data)

    def action(self):
        return


class Candidate(BaseState):
    def __str__(self):
        return '[Candidate::{}]'.format(self.identity)

    __repr__ = __str__

    def __init__(self, identity, term, committed_index, last_applied):
        super().__init__(identity, term, committed_index, last_applied)

    def routine(self, data):
        _state = super().routine(data)
        if type(_state) is Candidate:
            self.current_term += 1
            self.voted_for = self.identity
            _state = self
        return _state

    def action(self, peers, last_log_index, last_log_term, request_vote):
        print("{} raise a vote request for term: {}".format(self.identity, self.current_term))
        with ThreadPoolExecutor(max_workers=max(len(peers), 1)) as executor:
            futures = [executor.submit(request_vote,
                                       peer,
                                       {
                                           "term": self.current_term,
                                           "candidateId": self.identity,
                                           "lastLogIndex": last_log_index,
                                           "lastLogTerm": last_log_term
                                       })
                       for peer in peers]

        return len([1 for f in futures if f.result().voteGranted]) + 1 > (len(peers) + 1) // 2




