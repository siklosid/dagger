from hashlib import md5


def get_uid(*args):
    return md5("".join(args).encode()).hexdigest()
