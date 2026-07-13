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


class InvalidParameterError(RetentioneeringError):
    def __init__(self, param_name: str, value: str, allowed_values: list | None = None):
        message = f"Invalid value '{value}' for parameter '{param_name}'"
        if allowed_values:
            message += f". Allowed values: {allowed_values}"
        super().__init__(message, "INVALID_PARAMETER")


class SchemaConfigError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "SCHEMA_CONFIG_ERROR")


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


class InvalidMetricConfigError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "INVALID_METRIC_CONFIG")


class MetricDistributionError(RetentioneeringError):
    pass


class SegmentValueNotFoundError(MetricDistributionError):
    def __init__(
        self, segment_value: str, segment_col: str, available_values: list | None = None
    ):
        message = f"Segment value '{segment_value}' not found in column '{segment_col}'"
        if available_values:
            message += f". Available values: {available_values}"
        super().__init__(message, "SEGMENT_VALUE_NOT_FOUND")


class PathIdNotFoundError(RetentioneeringError):
    def __init__(self, path_ids: list, path_col: str):
        message = f"Path ID(s) {path_ids} not found in column '{path_col}'"
        super().__init__(message, "PATH_ID_NOT_FOUND")


class InvalidComplementConfigError(MetricDistributionError):
    def __init__(self, message: str):
        super().__init__(message, "INVALID_COMPLEMENT_CONFIG")


class WidgetExportError(RetentioneeringError):
    def __init__(self, message: str):
        super().__init__(message, "WIDGET_EXPORT_ERROR")
