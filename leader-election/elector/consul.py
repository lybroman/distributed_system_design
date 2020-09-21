from . import base
import consul


class ConsulElector(base.Elector):
    """
    1. https://www.consul.io/docs/dynamic-app-config/sessions
    2. https://clivern.com/leader-election-with-consul-and-golang/
    """
    def __init__(self, identity, election_path, lease_duration, host, port):
        super().__init__(identity, election_path)
        self.__cs = consul.Consul(host=host, port=port)
        self.__lease_duration = lease_duration
        # The contract of a TTL is that it represents a lower bound for invalidation;
        # that is, Consul will not expire the session before the TTL is reached,
        # but it is allowed to delay the expiration past the TTL.
        self.__session_id = self.__cs.session.create(
            name=self.election_path,
            behavior="delete",
            ttl=self.__lease_duration
        )

    def get_leader(self):
        index, value = self.__cs.kv.get(key=self.election_path)
        return value['Value'] if value else 'N/A'

    def is_leader(self):
        index, value = self.__cs.kv.get(key=self.election_path)
        return value['Value'].decode() == self.__session_id if value else False

    def renew_leadership(self):
        self.__cs.session.renew(self.__session_id)

    def acquire_leadership(self):
        self.__cs.session.renew(self.__session_id)
        self.__cs.kv.put(
            key=self.election_path,
            value=self.__session_id,
            acquire=self.__session_id)


if __name__ == '__main__':
    el = ConsulElector('test', 'sample_path', 10, '127.0.0.1', 8500)
    el.acquire_leadership()
    print(el.get_leader())
    print(el.is_leader())

