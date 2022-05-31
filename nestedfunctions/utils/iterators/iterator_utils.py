from typing import List


def distinct(sequence: List[str]) -> List[str]:
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]


def flatten(t: List):
    return [item for sublist in t for item in sublist]
