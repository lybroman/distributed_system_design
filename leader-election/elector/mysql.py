from . import base
from sqlalchemy import create_engine


class MySqlElector(base.Elector):
    """
    http://code.openark.org/blog/mysql/leader-election-using-mysql
    """
    def __init__(self, identity, election_path, lease_duration, username, password, host, port):
        super().__init__(identity, election_path)
        create_election_path_sql = f"""
        CREATE TABLE IF NOT EXISTS {election_path} ( 
          anchor tinyint(3) unsigned NOT NULL, 
          service_id varchar(128) NOT NULL, 
          last_seen_active timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, 
          PRIMARY KEY (anchor) 
        ) ENGINE=InnoDB
        """

        self.__engine = create_engine("mysql://{}:{}@{}/election".format(username, password, host),
                                      isolation_level="READ UNCOMMITTED")

        self.__lease_duration = lease_duration

        with self.__engine.connect() as conn:
            conn.execute(create_election_path_sql)

    def get_leader(self):
        get_leader_sql = f"""select max(service_id) as leader from {self.election_path} where anchor=1;"""
        with self.__engine.connect() as conn:
            rp = conn.execute(get_leader_sql)
            if rp.rowcount > 0:
                return rp.fetchone()[0]

    def is_leader(self):
        is_leader_sql = f"""select count(*) as is_leader from {self.election_path} 
               where anchor=1 and service_id='{self.identity}';"""

        with self.__engine.connect() as conn:
            rp = conn.execute(is_leader_sql)
            return rp.fetchone()[0] > 0

    def acquire_leadership(self):
        acquire_lock_sql = f"""
        insert ignore into {self.election_path} ( anchor, service_id, last_seen_active ) values ( 1, '{self.identity}', now() ) on duplicate key update service_id = if(last_seen_active < now() - interval {self.__lease_duration} second, values(service_id), service_id), last_seen_active = if(service_id = values(service_id), values(last_seen_active), last_seen_active);
        """

        with self.__engine.connect() as conn:
            rp = conn.execute(acquire_lock_sql)

    renew_leadership = acquire_leadership


if __name__ == '__main__':
    el = MySqlElector('test', 'sample_path', 20, 'root', 'root123', '127.0.0.1', '3306')
    el.acquire_leadership()
    print(el.is_leader())
    print(el.get_leader())
