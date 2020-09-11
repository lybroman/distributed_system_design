from .serializer import default_serializer


class BaseStorage(object):
    NotImplementedErrorMsg = "NotImplementedError"

    def __init__(self):
        # log entries; each entry contains command for state machine,
        # and term when entry was received by leader (first index is 1)
        self.logs = [
            {
                "msg": "$:$",
                "term": 0
            }
        ]

    @property
    def last_log_index(self):
        return len(self.logs) - 1

    @property
    def last_log_term(self):
        return self.get_log_term_by_index(self.last_log_index)

    def store_entry(self, entry):
        raise NotImplementedError(self.NotImplementedErrorMsg)

    def store_entries_from_index(self, index, entries):
        raise NotImplementedError(self.NotImplementedErrorMsg)

    def get_entries_from_index(self, index):
        raise NotImplementedError(self.NotImplementedErrorMsg)

    def delete_entry_to_index_exclusively(self, index):
        raise NotImplementedError(self.NotImplementedErrorMsg)

    def get_log_term_by_index(self, index):
        raise NotImplementedError(self.NotImplementedErrorMsg)


class RAMStorage(BaseStorage):
    def store_entry(self, entry):
        self.logs.append(entry)

    def get_log_term_by_index(self, index):
        if index >= len(self.logs) or index < 0:
            return -1
        else:
            return self.logs[index]['term']

    def delete_entry_to_index_exclusively(self, index):
        self.logs = self.logs[:max(0, index)]

    def store_entries_from_index(self, index, entries):
        self.logs = self.logs[:max(index + 1, 0)] + [default_serializer.loads(entry) for entry in entries]

    def get_entries_from_index(self, index):
        return [default_serializer.dumps(entry) for entry in self.logs[max(0, index):]]
