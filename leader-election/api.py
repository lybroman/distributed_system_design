import time
import threading
import sys


# refer to https://www.geeksforgeeks.org/python-different-ways-to-kill-a-thread/
# if using python 3.8, it would be much simpler to terminate the thread
class _Runner(threading.Thread):

    @staticmethod
    def wrapper(role):
        def _wrapper(f):
            def __wrapper(*args, **kwargs):
                try:
                    f(*args, **kwargs)
                except SystemExit:
                    # cleanup function could be triggered here
                    print(f"{role} routine Exited")
                except Exception as _:
                    raise
            return __wrapper
        return _wrapper

    def __init__(self, target, role, *args, **kwargs):
        threading.Thread.__init__(self, target=self.wrapper(role)(target), daemon=True, *args, **kwargs)
        self.__base_run = None
        self.__stopped = False

    def start(self):
        self.__base_run = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__base_run()
        self.run = self.__base_run

    def globaltrace(self, frame, event, arg):
        if event == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.__stopped:
            if event == 'line':
                raise SystemExit()
        return self.localtrace

    def terminate(self):
        self.__stopped = True


class Routine(threading.Thread):
    def __init__(self, elector, identity, renew_interval, leader_routine, follower_routine, **kwargs):
        super().__init__(daemon=True)
        self.__elector = elector(identity, **kwargs)
        self.__identity = identity
        self.__renew_interval = renew_interval
        self.__lrf = leader_routine
        self.__frf = follower_routine
        self.__th_obj = None

    def run(self):
        while True:
            self.__elector.acquire_leadership()
            if self.__elector.is_leader():
                if self.__th_obj:
                    self.__th_obj.terminate()

                self.__th_obj = _Runner(target=self.__lrf, role="leader")
                self.__th_obj.start()
                while True:
                    self.__elector.renew_leadership()
                    print(f"{self.__identity} send leader heartbeat @ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
                    time.sleep(self.__renew_interval)
                self.__th_obj.terminate()
                self.__th_obj = None

            if not self.__th_obj:
                self.__th_obj = _Runner(target=self.__frf, role="follower")
                self.__th_obj.start()

            time.sleep(self.__renew_interval)
            print(f"{self.__identity} send follower heartbeat @ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    def start(self):
        super().start()


if __name__ == '__main__':
    from elector.mysql import MySqlElector
    from elector.consul import ConsulElector
    from elector.zk import ZooKeeperElector
    import uuid

    def g(role):
        def f():
            while True:
                print(f"{role} routine is running...")
                time.sleep(1)
        return f

    # r = Routine(elector=MySqlElector,
    #             identity=str(uuid.uuid1()),
    #             renew_interval=3,
    #             leader_routine=g('leader'),
    #             follower_routine=g('follower'),
    #             lease_duration=10,
    #             election_path="election_path",
    #             username="root", password="root123",
    #             host="127.0.0.1",
    #             port="3306")


    # r = Routine(
    #     elector=ConsulElector,
    #     identity=str(uuid.uuid1()),
    #     renew_interval=3,
    #     leader_routine=g('leader'),
    #     follower_routine=g('follower'),
    #     lease_duration=10,
    #     election_path="election_path",
    #     host="127.0.0.1",
    #     port=8500
    # )

    r = Routine(
            elector=ZooKeeperElector,
            identity=str(uuid.uuid1()),
            renew_interval=3,
            leader_routine=g('leader'),
            follower_routine=g('follower'),
            lease_duration=10,
            election_path="election_path",
            host="127.0.0.1",
            port=2181
        )

    r.start()
    time.sleep(1024)
