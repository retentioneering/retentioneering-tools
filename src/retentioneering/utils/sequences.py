PATH_DELIMITER = "->"


def find_delimiter_collisions(names, delimiter=PATH_DELIMITER):
    """
    Return the subset of `names` that contain the literal path delimiter
    `delimiter`. Every `->`-joined path/pattern in this codebase
    (matches_pattern, step_matrix, this module's own gap expansion above)
    treats `delimiter` as a token boundary; an event name containing it
    becomes indistinguishable from multiple separate tokens once joined into
    a path string. Callers use this to reject such names up front, before
    they can produce a silent, incorrect pattern match.
    """
    return sorted({n for n in names if delimiter in n})


def generate_patterns_with_optional_gaps(pattern):
    """
    Generate all possible patterns with '.*' enabled or disabled. For example, we need to extend a given 'A->.*->B',
    with 'A->B' because '.*' might represent no events. Similarly, a pattern 'A->.*->B->.*->C', is extended with
    patterns 'A->B->C', 'A->.*->B->C', 'A->B->.*->C'.
    """
    # Split the pattern by '->' to process each part
    parts = pattern.split("->")

    # Recursive function to generate all combinations of '.*' enabled or disabled
    def helper(parts, index):
        if index >= len(parts):
            return [""]

        current_part = parts[index]
        # Recursively get patterns for the rest of the list
        rest_parts = helper(parts, index + 1)

        # For current part, generate patterns with and without '.*'
        if current_part == ".*":
            # Skip '.*' (disabled) or include '->.*->' (enabled)
            patterns = [f"->.*{rest}" for rest in rest_parts if rest] + rest_parts
        else:
            # Always include non-'.*' parts
            patterns = [f"->{current_part}{rest}" for rest in rest_parts]

        return patterns

    # Start the recursive function
    generated_patterns = helper(parts, 0)
    # Join patterns without leading '->'
    result = [pattern[2:] for pattern in generated_patterns if pattern]
    # Sorting the results by the first occurrence of '.*'. Patterns without '.*' should come first.
    result = sorted(result, key=lambda x: x.find(".*") if ".*" in x else -1)
    return result
