import unittest
from math_ops import add, subtract, multiply, divide

# class name TestMathOperations
# method names test_methodname
class TestMathOperations(unittest.TestCase):
    def test_add_positive_numbers(self):
        self.assertEqual(add(1, 2), 3)

    def test_add_negative_numbers(self):
        self.assertNotEqual(add(-1, -2), 4)

    def test_subtract_positive_numbers(self):
        self.assertTrue(subtract(5, 2) == 3)

    def test_subtract_negative_numbers(self):
        self.assertFalse(subtract(-1, -2) == 3)

    def test_multiply_positive_numbers(self):
        self.assertAlmostEqual(multiply(2.5, 2), 5.0, delta=0.0001)

    def test_multiply_negative_numbers(self):
        self.assertNotAlmostEqual(multiply(-2, -3), -6, delta=0.0001)

    def test_divide_positive_numbers(self):
        self.assertAlmostEqual(divide(10, 2), 5.0)

    def test_divide_by_zero(self):
        with self.assertRaises(ValueError):
            divide(2, 11)


if __name__ == '__main__':
    unittest.main()