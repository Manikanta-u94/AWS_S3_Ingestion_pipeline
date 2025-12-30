import unittest
from src.services.s3_to_db import S3ToDB


class TestS3ToDB(unittest.TestCase):

    def test_initialization(self):
        obj = S3ToDB(5, 10)

        self.assertEqual(obj.a, 5)
        self.assertEqual(obj.b, 10)

    def test_read_returns_sum_of_instance_values(self):
        obj = S3ToDB(3, 7)

        result = obj.read(100, 200)   # arguments ignored in current logic

        self.assertEqual(result, 10)

    def test_read_ignores_passed_arguments(self):
        obj = S3ToDB(2, 3)

        result = obj.read(999, 888)

        self.assertEqual(result, 5)

    def test_read_with_negative_and_float_values(self):
        obj1 = S3ToDB(-5, 5)
        obj2 = S3ToDB(-3, -2)
        obj3 = S3ToDB(10.5, 2.5)

        self.assertEqual(obj1.read(0, 0), 0)
        self.assertEqual(obj2.read(0, 0), -5)
        self.assertEqual(obj3.read(0, 0), 13.0)

    def test_transform_returns_none(self):
        obj = S3ToDB(1, 2)

        self.assertIsNone(obj.transform())

    def test_load_returns_none(self):
        obj = S3ToDB(1, 2)

        self.assertIsNone(obj.load())


if __name__ == "__main__":
    unittest.main()
