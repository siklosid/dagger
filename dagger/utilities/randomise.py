from random import choice
import string


def generate_random_name(length: int = 10):
    return ''.join([choice(string.ascii_lowercase) for i in range(length)])
