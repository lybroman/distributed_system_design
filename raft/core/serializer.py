class BaseSerializer(object):
    NotImplementedErrorMsg = "NotImplementedError"

    def dumps(self, obj):
        raise NotImplementedError(self.NotImplementedErrorMsg)

    def loads(self, string):
        raise NotImplementedError(self.NotImplementedErrorMsg)


class NaiveMapSerializer(object):
    def dumps(self, obj):
        if not type(obj) is dict:
            raise TypeError("Only map type is supported")
        return ";;".join(["{}:{}".format(k, v) for k, v in obj.items()])

    def loads(self, string):
        res = {}
        for kv in string.split(";;"):
            k, v = kv.split(":")
            res[k] = v if k != "term" else int(v)
        return res


default_serializer = NaiveMapSerializer()

if __name__ == "__main__":
    assert(default_serializer.loads(default_serializer.dumps({"k": "v"})) == {"k": "v"})
