import unittest

from dagger.utilities.classes import get_deep_obj_subclasses


class TestSubclasses(unittest.TestCase):
    def test_get_obj_deep_subclasses_empty(self):
        # Arrange
        class A:
            pass

        # Act
        subclasses = get_deep_obj_subclasses(A)

        # Assert
        self.assertListEqual(subclasses, [])

    def test_get_obj_deep_subclasses_one(self):
        # Arrange
        class A:
            pass

        class B(A):
            pass

        # Act
        subclasses = get_deep_obj_subclasses(A)

        # Assert
        self.assertListEqual(subclasses, [B])

    def test_get_obj_deep_subclasses_multiple(self):
        # Arrange
        class A:
            pass

        class B(A):
            pass

        class C(A):
            pass

        class X(B):
            pass

        class Y(B):
            pass

        class Z(C):
            pass

        # Act
        subclasses = get_deep_obj_subclasses(A)

        # Assert
        self.assertListEqual(subclasses, [B, C, X, Y, Z])


if __name__ == "__main__":
    unittest.main()
