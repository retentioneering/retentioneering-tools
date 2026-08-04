PATH_DELIMITER = "->"


def find_delimiter_collisions(names, delimiter=PATH_DELIMITER):
    """
    Return the subset of `names` that contain the literal path delimiter
    `delimiter`. Every `->`-joined path/pattern in this codebase
    (`paths.anchors`, and the `matches_pattern` metric and Step Matrix
    `path_pattern` that build on it) treats `delimiter` as a token boundary;
    an event name containing it becomes indistinguishable from multiple
    separate tokens once split into tokens. Callers use this to reject such
    names up front, before they can produce a silent, incorrect pattern match.
    """
    return sorted({n for n in names if delimiter in n})
