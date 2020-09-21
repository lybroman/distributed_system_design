NotImplementedErrorMsg = 'Method not implemented by derived class'


class Elector(object):
    def __init__(self, identity, election_path, *args, **kwargs):
        self.identity = identity
        self.election_path = election_path

    def get_leader(self):
        raise NotImplementedError(NotImplementedErrorMsg)

    def is_leader(self):
        raise NotImplementedError(NotImplementedErrorMsg)

    def acquire_leadership(self):
        raise NotImplementedError(NotImplementedErrorMsg)

    def renew_leadership(self):
        raise NotImplementedError(NotImplementedErrorMsg)