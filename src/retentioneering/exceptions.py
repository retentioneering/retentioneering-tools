import reprlib


_SHORT_REPR = reprlib.Repr()
_SHORT_REPR.maxstring = 120
_SHORT_REPR.maxother = 120
_SHORT_REPR.maxlist = _SHORT_REPR.maxtuple = _SHORT_REPR.maxset = 8
_SHORT_REPR.maxdict = 8


def _short_repr(value: object) -> str:
    """``repr`` for error messages: a bad value should be recognisable, but a
    thousand-element list or a whole DataFrame must not become the message."""
    return _SHORT_REPR.repr(value)


class RetentioneeringError(Exception):
    def __init__(self, message: str, error_code: str):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class EmptyEventstreamError(RetentioneeringError):
    def __init__(self, context: str | None = None):
        message = "Eventstream is empty"
        if context:
            message += f": {context}"
        super().__init__(message, "EMPTY_EVENTSTREAM")


class DiffConfigError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "DIFF_CONFIG_ERROR")


class PatternSyntaxError(RetentioneeringError):
    """A path pattern that cannot be parsed at all.

    Distinct from `InvalidParameterError`, which reports a *known* parameter
    holding an unknown *value* and renders the vocabulary of allowed values —
    the right shape for "this event does not exist", the wrong one for "these
    brackets do not balance", where the useful message is free-form prose
    naming the construct that is supported instead.
    """

    def __init__(self, message: str):
        super().__init__(message, "PATTERN_SYNTAX")


class InvalidParameterError(RetentioneeringError):
    def __init__(self, param_name: str, value: str, allowed_values: list | None = None):
        message = f"Invalid value '{value}' for parameter '{param_name}'"
        if allowed_values:
            message += f". Allowed values: {allowed_values}"
        super().__init__(message, "INVALID_PARAMETER")


class SchemaConfigError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "SCHEMA_CONFIG_ERROR")

    @classmethod
    def invalid_parameter(
        cls, parameter: str, expected: str, value: object, hint: str = ""
    ) -> "SchemaConfigError":
        """Describe an invalid schema parameter without losing its type or value.

        ``hint`` is appended verbatim (e.g. a suggested correct spelling).
        """
        return cls(
            f"Schema key '{parameter}' must be {expected}; "
            f"got {type(value).__name__} {_short_repr(value)}.{hint}"
        )


class PreprocessingConfigError(RetentioneeringError):
    def __init__(self, processor: str, message: str):
        super().__init__(f"[{processor}] {message}", "PREPROCESSING_CONFIG_ERROR")


class PreprocessingColumnNotFoundError(RetentioneeringError):
    def __init__(self, processor: str, column: str, available: list):
        super().__init__(
            f"[{processor}] Column '{column}' not found. Available: {available}",
            "PREPROCESSING_COLUMN_NOT_FOUND",
        )


class PatternNoMatchError(RetentioneeringError):
    def __init__(self, pattern: str, group: str | None = None):
        msg = f"Pattern '{pattern}' doesn't match any paths"
        if group:
            msg += f" in {group}"
        super().__init__(msg, "PATTERN_NO_MATCH")


class GridPointNotFoundError(RetentioneeringError):
    """`select=` named a point the searched grid does not contain.

    Its own class rather than a bare ValueError because the Cluster Analysis
    widget has to tell it apart: a selection restored from a state file can name
    a point of an older grid, and that is a stale preference to drop, not a
    failure to surface.
    """

    def __init__(self, select: dict, available: list):
        super().__init__(
            f"select={select!r} matches no point of the searched grid. "
            f"Available: {available!r}",
            "GRID_POINT_NOT_FOUND",
        )
        self.select = select
        self.available = available


class AmbiguousGridPointError(RetentioneeringError):
    """`select=` named several points of the searched grid at once.

    Separate from GridPointNotFoundError because it must never be swallowed:
    silently interpreting whichever match came first would answer a question the
    caller did not ask, and the two candidates can differ arbitrarily.
    """

    def __init__(self, select: dict, matches: list):
        super().__init__(
            f"select={select!r} matches {len(matches)} points of the searched "
            f"grid: {matches!r}. Name every searched parameter to pick one.",
            "AMBIGUOUS_GRID_POINT",
        )
        self.select = select
        self.matches = matches


class InvalidMetricConfigError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "INVALID_METRIC_CONFIG")


class MetricDistributionError(RetentioneeringError):
    pass


class SegmentLevelNotFoundError(MetricDistributionError):
    def __init__(
        self, segment_level: str, segment_col: str, available_levels: list | None = None
    ):
        message = f"Segment level '{segment_level}' not found in column '{segment_col}'"
        if available_levels:
            message += f". Available levels: {available_levels}"
        super().__init__(message, "SEGMENT_LEVEL_NOT_FOUND")


class PathIdNotFoundError(RetentioneeringError):
    def __init__(self, path_ids: list, path_col: str):
        message = f"Path ID(s) {path_ids} not found in column '{path_col}'"
        super().__init__(message, "PATH_ID_NOT_FOUND")


class InvalidSegmentSelectionError(MetricDistributionError):
    def __init__(self, message: str):
        super().__init__(message, "INVALID_SEGMENT_SELECTION")


class WidgetExportError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "WIDGET_EXPORT_ERROR")
