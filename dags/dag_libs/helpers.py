import re


def slugify(s):
    return re.sub(r'[^A-Za-z0-9]', '_', s).lower()
