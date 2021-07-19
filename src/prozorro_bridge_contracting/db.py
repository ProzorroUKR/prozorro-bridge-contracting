import redis


class Db:
    """ Database proxy """

    def __init__(self, config):
        self.config = config

        self._backend = None
        self._db_id = None
        self._port = None
        self._host = None

        if "cache_host" in self.config:
            self._backend = "redis"
            self._host = self.config.get("cache_host")
            self._port = self.config.get("cache_port", 6379)
            self._db_id = self.config.get("cache_db_id", 0)
            self.db = redis.Redis(host=self._host, port=self._port, db=self._db_id)
            self.set_value = self.db.set
            self.has_value = self.db.exists

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        self.set_value(key, value)

    def has(self, key):
        return self.has_value(key)
