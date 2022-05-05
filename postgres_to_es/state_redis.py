import abc
from redis import Redis
from typing import Any


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> bytes:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):

    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, key, value):
        self.redis_adapter.set(key, value)

    def retrieve_state(self, name):
        return self.redis_adapter.get(name)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: RedisStorage):
        self.storage = storage
        self.d = {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.d[key] = value
        self.storage.save_state(key, value)

    def get_state(self, key: str) -> Any:
        retr_state = self.storage.retrieve_state(key)
        if retr_state is None:
            return None
        else:
            return retr_state.decode("utf-8")


if __name__ == '__main__':
    red_conn = Redis(host='localhost', port=6379, db=0)
    r = RedisStorage(red_conn)
    s = State(r)
    s.set_state('test_1', 'redis')
    res = s.get_state('test_2')
    print(res)

