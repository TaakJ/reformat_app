from abc import ABC, abstractmethod

class record_log(ABC):
    def __init__(self):
        self._log = []

    @property
    def logging(self) -> list:
        return self._log

    @logging.setter
    def logging(self, log: list) -> None:
        self._log_setter(log)

    @abstractmethod
    def _log_setter(self, log: list):
        pass