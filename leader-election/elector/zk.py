from . import base
from kazoo.client import KazooClient


class ZooKeeperElector(base.Elector):
    """
    https://www.tutorialspoint.com/zookeeper/zookeeper_leader_election.htm
    """
    def __init__(self, identity, election_path, lease_duration, host, port):
        super().__init__(identity, election_path)
        self.__zc = KazooClient(hosts=f'{host}:{port}')
        self.__zc.start()
        try:
            self.__zc.create(
                path=self.election_path,
                value=self.identity.encode()
            )
        except Exception as e:
            print(e)

        self.__cid = self.__zc.create(
            path=self.election_path + '/client-',
            value=self.identity.encode(),
            ephemeral=True,
            sequence=True
        ).split('/')[-1]

    def get_leader(self):
        return self.__zc.get_children(path=self.election_path)

    def is_leader(self):
        return self.__zc.get_children(path=self.election_path)[0] == self.__cid

    def renew_leadership(self):
        pass

    def acquire_leadership(self):
        pass


if __name__ == '__main__':
    el = ZooKeeperElector('test', 'sample_path', 10, '127.0.0.1', 2181)
    el.acquire_leadership()
    print(el.get_leader())
    print(el.is_leader())

