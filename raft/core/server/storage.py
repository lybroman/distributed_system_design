class BaseStorage(object):
    def __init__(self):
        # log entries; each entry contains command for state machine,
        # and term when entry was received by leader (first index is 1)
        self.logs = ["$#$"]
        self.idx2term = {0: 0}

    @property
    def last_log_index(self):
        return len(self.logs) - 1

    @property
    def last_log_term(self):
        return self.get_log_term_by_index(self.last_log_index)

    def store_entry(self, entry):
        raise NotImplementedError("NotImplementedError")

    def store_entries_from_index(self, index, term, entries):
        raise NotImplementedError("NotImplementedError")

    def get_entries_from_index(self, index):
        raise NotImplementedError("NotImplementedError")

    def delete_entry_to_index_exclusively(self, index):
        raise NotImplementedError("NotImplementedError")

    def get_log_term_by_index(self, index):
        raise NotImplementedError("NotImplementedError")


class RAMStorage(BaseStorage):
    def store_entry(self, entry):
        self.logs.append(entry)

    def get_log_term_by_index(self, index):
        return self.idx2term.get(index, -1)

    def delete_entry_to_index_exclusively(self, index):
        self.logs = self.logs[:max(0, index)]

    def store_entries_from_index(self, index, term, entries):
        start_idx = max(index + 1, 0)
        for i, entry in enumerate(entries):
            self.idx2term[i + start_idx] = term
            self.logs.insert(i + start_idx, entry)

    def get_entries_from_index(self, index):
        return self.logs[max(0, index):]
