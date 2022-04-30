import abc
import json
import os
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, obj):
        with open(os.path.join(self.file_path, 'data.json'), "w") as fp:
            json.dump(obj, fp)

    def retrieve_state(self,):
        if os.path.exists(os.path.join(self.file_path, 'data.json')):
            with open(os.path.join(self.file_path, 'data.json'), "r") as fp:
                return json.load(fp)
        else:
            return dict()


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage
        self.d = {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.d[key] = value
        self.storage.save_state(self.d)

    def get_state(self, key: str) -> Any:
        j = self.storage.retrieve_state()
        value = j.get(key, None)
        if value is None:
            return None
        else:
            return value
