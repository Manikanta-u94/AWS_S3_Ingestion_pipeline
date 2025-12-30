
from src.abstracts.etl_abstract import ETlAbstract


class S3ToDB(ETlAbstract):

    def __init__(self,a,b):
        self.a = a
        self.b = b

    def read(self,a,b):
        c = self.a + self.b
        return c

    def transform(self):
        pass

    def load(self):
        pass