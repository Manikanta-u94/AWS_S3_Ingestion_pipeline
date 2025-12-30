from abc import ABC, abstractmethod


class ETlAbstract(ABC):

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
