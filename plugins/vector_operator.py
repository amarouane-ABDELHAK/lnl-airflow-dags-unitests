from airflow.models.baseoperator import BaseOperator


def validate_input(vectors: list, action):
    print(vectors)
    for vector in vectors:
        if len(vector) != 2:
            raise Exception("We only deal with 2D vectors")
    if len({"addition", "subtraction", action}) != 2:
        raise Exception("We only support addition and subtraction")


class VectorOperator(BaseOperator):
    def __init__(self, vector1: set, vector2: set, action: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.vector1 = vector1
        self.vector2 = vector2
        self.action = action

    @staticmethod
    def __add_vectors(v1x, v1y, v2x, v2y):
        v3 = (v1x + v2x,
              v1y + v2y)
        return v3

    @staticmethod
    def __subtract_vectors(v1x, v1y, v2x, v2y):
        v3 = (v1x - v2x,
              v1y - v2y)
        return v3

    def execute(self, context):
        validate_input([self.vector1, self.vector2], self.action)
        v1x, v1y = self.vector1
        v2x, v2y = self.vector2
        action_funct = {
            "addition": self.__add_vectors,
            "subtraction": self.__subtract_vectors
        }
        result = action_funct[self.action](v1x, v1y, v2x, v2y)
        message = f" The result of the operation {self.action} is {result}"
        print(message)
        return result
