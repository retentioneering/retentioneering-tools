"""
SegmentOverview - tool for analyzing metric distributions across segment levels

Provides aggregated view of metrics for each segment level, with support for:
- Basic statistics: mean, median, percentiles (q5, q25, q75, q95)
- Distributional comparison: complement_distance (Wasserstein distance between
  segment level distribution and its complement)
- Special per-segment metrics: segment_size, segment_share (always included)
- metric_distribution: detailed distribution analysis with histogram, KDE, etc.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde, wasserstein_distance

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


from retentioneering.exceptions import (
    InvalidMetricConfigError,  # Used in tests, re-raised from MetricBuilder
    InvalidParameterError,
    InvalidSegmentSelectionError,
    SegmentLevelNotFoundError,
)
from retentioneering.metrics.metric_builder import MetricBuilder, MetricConfig
from retentioneering.utils.sentinels import UNSET as _UNSET

# Threshold for considering metric as discrete (use bar chart instead of histogram)
DISCRETE_THRESHOLD = 10

# Maximum number of bins for histogram
MAX_BINS = 50

# Thresholds for auto log-scale detection
LOG_SCALE_RANGE_RATIO = 1000  # If max/min > this ratio, consider log scale
LOG_SCALE_SKEWNESS = 3.0  # If skewness > this, consider log scale
LOG_SCALE_MIN_POSITIVE = 1e-10  # Minimum value to add when log-transforming zeros


# Mapping from agg name to pandas aggregation function
AGG_FUNCTIONS = {
    "mean": "mean",
    "median": "median",
    "q5": lambda x: x.quantile(0.05),
    "q25": lambda x: x.quantile(0.25),
    "q75": lambda x: x.quantile(0.75),
    "q95": lambda x: x.quantile(0.95),
}


def _build_composite_path(
    df: pd.DataFrame, path_col: str, segment_col: str
) -> Tuple[np.ndarray, Dict[Any, Any]]:
    """
    Build an integer composite path id combining ``path_col`` and ``segment_col``,
    so MetricBuilder can compute per-path metrics separately for each segment
    value.

    This uses `factorize` rather than the previous `astype(str) + sep +
    astype(str)` string concatenation, which had two failure modes: numeric
    segment levels came back stringified (e.g. `1.0` instead of `1`), and rows
    with a missing (None/NaN) segment level — which `add_segment` can produce
    via its `func`/`sql` modes — silently vanished, since concatenating a NaN
    into a string produces NaN for the whole composite key. `path_col` is
    already guaranteed non-null by Eventstream's own validation, and
    `use_na_sentinel=False` gives every distinct segment level (including a
    missing one) its own stable code, so the composite id is always defined.

    Returns the composite id array and a mapping from composite id back to the
    original segment level (None for a missing segment, the original level
    otherwise).
    """
    segment_codes, segment_uniques = pd.factorize(
        df[segment_col], use_na_sentinel=False
    )
    pairs = pd.array(list(zip(df[path_col], segment_codes)), dtype=object)
    composite_ids, _ = pd.factorize(pairs)
    segment_by_composite = dict(
        zip(
            composite_ids,
            (
                None if pd.isna(segment_uniques[code]) else segment_uniques[code]
                for code in segment_codes
            ),
        )
    )
    return composite_ids, segment_by_composite


def _segment_mask(series: pd.Series, value: Any) -> pd.Series:
    """Mask matching `value` in `series`, treating None/NaN as their own group."""
    if pd.isna(value):
        return series.isna()
    return series == value


@dataclass
class SegmentOverview:
    """Builds overview of metric distributions across segment levels"""

    eventstream: "Eventstream"

    def fit(
        self,
        segment_col: str,
        metrics: List[Dict[str, Any]],
        path_col: str | None = None,
    ) -> pd.DataFrame:
        """
        Build segment overview DataFrame.

        Args:
            segment_col: Name of the segment column to analyze
            metrics: List of metric configuration dicts with keys:
                - 'metric': metric name (same as MetricBuilder)
                - 'metric_args': optional metric arguments (same as MetricBuilder)
                - 'agg': aggregation type ('mean', 'median', 'complement_distance',
                         'q5', 'q25', 'q75', 'q95')

            segment_size and segment_share are always computed automatically.

            path_col: Path ID column (if None, taken from schema)

        Returns:
            DataFrame with:
                - Rows: metric names (segment_size and segment_share always first)
                - Columns: unique segment levels
                - Values: aggregated metric values for each segment
        """
        path_col = path_col or self.eventstream.schema.path_col
        event_col = self.eventstream.schema.event_col
        df = self.eventstream.df.copy()

        if segment_col not in df.columns:
            raise ValueError(f"Segment column '{segment_col}' not found in DataFrame")

        # Create composite path ID: (path_id, segment_level)
        composite_col = "__composite_path_id__"
        composite_ids, segment_by_composite = _build_composite_path(
            df, path_col, segment_col
        )
        df[composite_col] = composite_ids

        # Create a temporary eventstream with composite path ID
        from retentioneering.eventstream.eventstream import Eventstream

        temp_schema = dict(self.eventstream._schema) if self.eventstream._schema else {}
        temp_schema["path_cols"] = [composite_col]
        temp_stream = Eventstream(df, temp_schema, preprocess=False)

        # Use MetricConfig to get enriched configs with column names. Pass the
        # available events so that has_event/event_count with omitted 'events'
        # (the "all events" wildcard) resolves to its real per-event column names
        # instead of an empty 'cols' list - otherwise those columns silently fall
        # back to the default "mean" aggregation below regardless of the
        # requested 'agg'.
        available_events = set(df[event_col].unique().tolist())
        metric_cfg = MetricConfig(metrics, available_events=available_events)
        enriched_config = metric_cfg.get_enriched_configs()

        # Build column-to-agg mapping
        column_to_agg: Dict[str, str] = {}
        for metric_config in enriched_config:
            agg = metric_config.get("agg", "mean")
            for col in metric_config.get("cols", []):
                column_to_agg[col] = agg

        # Build metrics using MetricBuilder
        if metrics:
            metric_builder = MetricBuilder(temp_stream)
            metrics_df = metric_builder.build_metrics(
                config=metrics,
                path_col=composite_col,
            )
            metrics_df[segment_col] = metrics_df.index.map(segment_by_composite)
        else:
            unique_composite_ids = df[composite_col].unique()
            metrics_df = pd.DataFrame(index=unique_composite_ids)
            metrics_df[segment_col] = metrics_df.index.map(segment_by_composite)

        metric_columns = [col for col in metrics_df.columns if col != segment_col]
        total_paths = len(metrics_df)

        # Separate columns by aggregation type
        standard_agg_cols = {}  # {col: agg_func} for standard aggregations
        complement_distance_cols = []  # columns requiring complement_distance

        for col in metric_columns:
            agg = column_to_agg.get(col, "mean")
            if agg == "complement_distance":
                complement_distance_cols.append(col)
            elif agg in AGG_FUNCTIONS:
                standard_agg_cols[col] = AGG_FUNCTIONS[agg]
            else:
                raise ValueError(f"Unknown aggregation type: {agg}")

        # Compute segment_size and segment_share. dropna=False keeps paths with a
        # missing (None/NaN) segment level as their own group instead of
        # silently excluding them.
        grouped = metrics_df.groupby(segment_col, dropna=False)
        segment_sizes = grouped.size()

        result_data = {
            "segment_size": segment_sizes,
            "segment_share": segment_sizes / total_paths
            if total_paths > 0
            else segment_sizes * 0,
        }

        # Aggregate standard columns using groupby.agg()
        if standard_agg_cols:
            aggregated = grouped.agg(standard_agg_cols)
            for col in standard_agg_cols:
                agg_name = column_to_agg.get(col, "mean")
                result_data[f"{col}_{agg_name}"] = aggregated[col]

        # Compute complement_distance separately (needs full dataframe)
        for col in complement_distance_cols:
            complement_distance_values = {}
            for segment_level in segment_sizes.index:
                mask = _segment_mask(metrics_df[segment_col], segment_level)
                segment_data = metrics_df.loc[mask, col].dropna()
                complement_data = metrics_df.loc[~mask, col].dropna()

                if len(segment_data) > 0 and len(complement_data) > 0:
                    complement_distance_values[segment_level] = wasserstein_distance(
                        segment_data.values, complement_data.values
                    )
                else:
                    complement_distance_values[segment_level] = np.nan

            result_data[f"{col}_complement_distance"] = pd.Series(
                complement_distance_values
            )

        # Create result DataFrame (metrics as rows, segments as columns)
        result_df = pd.DataFrame(result_data).T
        result_df.index.name = "metric"

        # groupby always represents a missing segment via its own internal NaN
        # sentinel; normalize it to a real `None` column label so callers can
        # reliably test `col is None` and JSON serialization doesn't choke on a
        # bare NaN. dtype=object is required here: assigning a plain list back
        # to .columns lets pandas re-infer the Index dtype, which silently
        # coerces None back into its own NaN sentinel when mixed with strings.
        result_df.columns = pd.Index(
            [None if pd.isna(c) else c for c in result_df.columns], dtype=object
        )

        # Sort columns for consistent output (None sorts last). List-based
        # column selection (`df[[...]]`) normalizes a bare `None` key back to
        # NaN and fails to find it, unlike scalar `.get_loc`, so reorder
        # positionally instead.
        sorted_columns = sorted(result_df.columns, key=lambda v: (v is None, v))
        column_order = [result_df.columns.get_loc(c) for c in sorted_columns]
        result_df = result_df.iloc[:, column_order]

        return result_df

    def get_metric_distribution(
        self,
        segment_col: str,
        metric: Dict[str, Any],
        *,
        segment_level: Any = _UNSET,
        segment_levels: List[Any] | None = None,
        path_col: str | None = None,
    ) -> Dict[str, Any]:
        """
        Calculate distribution statistics for a metric across one or two segment levels.

        Args:
            segment_col: Name of the segment column
            metric: Metric configuration dict with 'metric' and optional 'metric_args'
            segment_level: One level, compared against its complement — every
                other level of `segment_col`. `None` is a level in its own right
                (paths with a missing segment level), which is why "not given"
                is a sentinel rather than `None`.
            segment_levels: Two levels, compared against each other.
            path_col: Path ID column (if None, taken from schema)

        Exactly one of `segment_level` / `segment_levels` is required.

        Returns:
            For a single segment level (vs. its complement) and for a pair alike:
                {"distribution_1": {...}, "distribution_2": {...}, "distance": float}
        """
        df = self.eventstream.df.copy()

        # Validate path_col
        path_col = path_col or self.eventstream.schema.path_col
        if path_col not in df.columns:
            raise InvalidParameterError(
                "path_col",
                path_col,
                allowed_values=self.eventstream.schema.path_cols,
            )

        # Validate segment_col
        if segment_col not in df.columns:
            raise InvalidParameterError(
                "segment_col",
                segment_col,
                allowed_values=self.eventstream.schema.segment_cols,
            )

        # Pick the comparison mode. `segment_level` may legitimately be None (paths
        # with a missing segment level), so "was it given at all" is the sentinel,
        # not the value.
        given = (segment_level is not _UNSET, segment_levels is not None)
        if sum(given) != 1:
            raise InvalidSegmentSelectionError(
                "Provide exactly one of: 'segment_level' (one level, compared with "
                "every other level of the segment column) or 'segment_levels' "
                "(two levels, compared with each other)."
            )

        if segment_levels is not None:
            if (
                not isinstance(segment_levels, (list, tuple))
                or len(segment_levels) != 2
            ):
                raise InvalidSegmentSelectionError(
                    f"'segment_levels' must be a list of exactly two levels, got "
                    f"{segment_levels!r}. To compare one level with the rest of the "
                    f"segment column, use 'segment_level' instead."
                )
            segment_levels = list(segment_levels)
            use_complement = False
        else:
            segment_levels = [segment_level]
            use_complement = True

        # Validate segment levels exist. NaN/None both mean "missing segment
        # level", so normalize both sides before comparing.
        available_segment_levels = [
            None if pd.isna(v) else v for v in df[segment_col].unique().tolist()
        ]
        segment_levels = [None if pd.isna(sv) else sv for sv in segment_levels]
        for sv in segment_levels:
            if sv not in available_segment_levels:
                raise SegmentLevelNotFoundError(
                    segment_level=sv,
                    segment_col=segment_col,
                    available_levels=available_segment_levels,
                )

        # Validate metric configuration using MetricBuilder
        MetricBuilder(self.eventstream).validate_metric_config(metric)

        # Create composite path ID: (path_id, segment_level)
        composite_col = "__composite_path_id__"
        composite_ids, segment_by_composite = _build_composite_path(
            df, path_col, segment_col
        )
        df[composite_col] = composite_ids

        # Create a temporary eventstream with composite path ID
        from retentioneering.eventstream.eventstream import Eventstream

        temp_schema = dict(self.eventstream._schema) if self.eventstream._schema else {}
        temp_schema["path_cols"] = [composite_col]
        temp_stream = Eventstream(df, temp_schema, preprocess=False)

        # Build metric using MetricBuilder
        metric_builder = MetricBuilder(temp_stream)
        metrics_df = metric_builder.build_metrics(
            config=[metric],
            path_col=composite_col,
        )
        metrics_df[segment_col] = metrics_df.index.map(segment_by_composite)

        # Get the metric column name - must be exactly one
        metric_cols = [col for col in metrics_df.columns if col != segment_col]
        if len(metric_cols) == 0:
            raise InvalidMetricConfigError(
                "Metric configuration did not produce any metric columns"
            )
        if len(metric_cols) > 1:
            raise InvalidMetricConfigError(
                f"metric_distribution requires exactly one metric, but the configuration "
                f"produced {len(metric_cols)} metrics: {metric_cols}. "
                f"For metrics like 'event_count_bulk'/'has_event_bulk', specify "
                f"metric_args={{'events': [...]}} with exactly one event, or use the "
                f"non-bulk 'event_count'/'has_event' metric. For 'in_segment_bulk', use "
                f"'in_segment' with a single 'segment_level'."
            )
        metric_col = metric_cols[0]

        # Get data for segment levels. Both modes end in a pair: a lone level is
        # paired with its complement.
        mask_1 = _segment_mask(metrics_df[segment_col], segment_levels[0])
        data_1 = metrics_df.loc[mask_1, metric_col].dropna().values
        if use_complement:
            data_2 = metrics_df.loc[~mask_1, metric_col].dropna().values
        else:
            mask_2 = _segment_mask(metrics_df[segment_col], segment_levels[1])
            data_2 = metrics_df.loc[mask_2, metric_col].dropna().values
        return self._build_pair_distribution(data_1, data_2)

    def _is_discrete(self, data: np.ndarray) -> bool:
        """Check if data should be treated as discrete (bar chart) or continuous (histogram)"""
        unique_values = np.unique(data)
        return len(unique_values) <= DISCRETE_THRESHOLD

    def _should_use_log_scale(self, data: np.ndarray) -> bool:
        """
        Determine if log scale should be used for the data.

        Log scale is recommended when:
        - Data has large range (max/min ratio > LOG_SCALE_RANGE_RATIO)
        - Data is highly skewed (skewness > LOG_SCALE_SKEWNESS)
        - All values are non-negative (can't log negative values)
        """
        if len(data) < 2:
            return False

        # Can't use log scale if there are negative values
        if data.min() < 0:
            return False

        # Check range ratio (for positive values)
        positive_data = data[data > 0]
        if len(positive_data) > 0:
            range_ratio = positive_data.max() / positive_data.min()
            if range_ratio > LOG_SCALE_RANGE_RATIO:
                return True

        # Check skewness
        mean = np.mean(data)
        std = np.std(data)
        if std > 0:
            skewness = np.mean(((data - mean) / std) ** 3)
            if skewness > LOG_SCALE_SKEWNESS:
                return True

        return False

    def _log_transform(
        self, data: np.ndarray, offset: float | None = None
    ) -> np.ndarray:
        """
        Apply log transformation to data, handling zeros.

        For zeros, we substitute a small positive value before taking log.
        If ``offset`` is given, it is used as that substitute; otherwise it is
        derived from ``data`` (half of its minimum positive value). Passing a
        shared offset keeps identical raw values aligned across related arrays
        (e.g. the two groups of a pair distribution).
        """
        if offset is None:
            offset = self._log_zero_offset(data)

        # Replace zeros with offset, then take log
        data_transformed = np.where(data > 0, data, offset)
        return np.log10(data_transformed)

    @staticmethod
    def _log_zero_offset(data: np.ndarray) -> float:
        """Compute the zero-replacement offset for log transform of ``data``."""
        positive_data = data[data > 0]
        if len(positive_data) > 0:
            # Use half of min positive value for zeros
            return float(positive_data.min()) / 2
        return LOG_SCALE_MIN_POSITIVE

    def _build_histogram(
        self, data: np.ndarray, bins: np.ndarray | None = None, max_bins: int = MAX_BINS
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Build histogram for data with limited number of bins.

        Returns:
            (bin_edges, counts)
        """
        if bins is None:
            # First try auto bins
            counts, bin_edges = np.histogram(data, bins="auto")
            # If too many bins, limit to max_bins
            if len(bin_edges) - 1 > max_bins:
                counts, bin_edges = np.histogram(data, bins=max_bins)
        else:
            counts, bin_edges = np.histogram(data, bins=bins)
        return bin_edges, counts

    def _build_discrete_histogram(
        self, data: np.ndarray, unique_values: np.ndarray | None = None
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Build bar chart style histogram for discrete data.
        Creates bins centered on integer values: [-0.5, 0.5, 1.5, ...] for values [0, 1, ...]

        Returns:
            (bin_edges, counts)
        """
        if unique_values is None:
            unique_values = np.unique(data)

        # Sort unique values
        unique_values = np.sort(unique_values)

        # For values [0, 1, 2], bins should be [-0.5, 0.5, 1.5, 2.5]
        bin_edges = np.concatenate([[unique_values[0] - 0.5], unique_values + 0.5])

        counts, _ = np.histogram(data, bins=bin_edges)
        return bin_edges, counts

    def _build_kde(
        self, data: np.ndarray, n_points: int = 1000
    ) -> List[List[float]] | None:
        """
        Build kernel density estimate for data.

        Returns:
            [[x_values], [y_values]] or None if KDE cannot be computed
        """
        if len(data) < 2:
            return None

        try:
            kde = gaussian_kde(data)
            x_min, x_max = data.min(), data.max()

            # Add some padding to the range
            padding = (x_max - x_min) * 0.1
            if padding == 0:
                padding = 0.5  # Handle case where all values are the same

            x_values = np.linspace(x_min - padding, x_max + padding, n_points)
            y_values = kde(x_values)

            return [x_values.tolist(), y_values.tolist()]
        except Exception:
            # KDE can fail for various reasons (e.g., singular covariance matrix)
            return None

    def _build_pair_distribution(
        self, data_1: np.ndarray, data_2: np.ndarray
    ) -> Dict[str, Any]:
        """Build distribution statistics for a pair of data arrays with shared bins"""
        if len(data_1) == 0 and len(data_2) == 0:
            empty_dist = {
                "bins": [],
                "counts": [],
                "counts_normalized": [],
                "kde": None,
                "mean": float("nan"),
                "median": float("nan"),
            }
            return {
                "distribution_1": empty_dist,
                "distribution_2": empty_dist,
                "distance": float("nan"),
                "log_scale": False,
            }

        # Combine data to determine if discrete and to compute shared bins
        combined_data = (
            np.concatenate([data_1, data_2])
            if len(data_1) > 0 and len(data_2) > 0
            else (data_1 if len(data_1) > 0 else data_2)
        )
        is_discrete = self._is_discrete(combined_data)

        # Determine if we should use log scale (based on combined data)
        log_scale = not is_discrete and self._should_use_log_scale(combined_data)

        # Transform data if using log scale.
        # Use a single zero-replacement offset derived from the combined data so
        # identical raw zeros map to the same transformed value in both groups.
        if log_scale:
            offset = self._log_zero_offset(combined_data)
            data_1_hist = (
                self._log_transform(data_1, offset) if len(data_1) > 0 else data_1
            )
            data_2_hist = (
                self._log_transform(data_2, offset) if len(data_2) > 0 else data_2
            )
            combined_hist = self._log_transform(combined_data, offset)
        else:
            data_1_hist = data_1
            data_2_hist = data_2
            combined_hist = combined_data

        if is_discrete:
            unique_values = np.unique(combined_hist)
            # Use shared bins
            shared_bins = self._build_discrete_histogram(combined_hist, unique_values)[
                0
            ]
            if len(data_1_hist) > 0:
                counts_1, _ = np.histogram(data_1_hist, bins=shared_bins)
            else:
                counts_1 = np.zeros(len(shared_bins) - 1, dtype=int)
            if len(data_2_hist) > 0:
                counts_2, _ = np.histogram(data_2_hist, bins=shared_bins)
            else:
                counts_2 = np.zeros(len(shared_bins) - 1, dtype=int)
            bins_1 = bins_2 = shared_bins
            kde_1 = None
            kde_2 = None
        else:
            # Compute shared bins based on combined data with max_bins limit
            _, shared_bins = np.histogram(combined_hist, bins="auto")
            if len(shared_bins) - 1 > MAX_BINS:
                _, shared_bins = np.histogram(combined_hist, bins=MAX_BINS)

            if len(data_1_hist) > 0:
                bins_1, counts_1 = self._build_histogram(data_1_hist, shared_bins)
                kde_1 = self._build_kde(data_1_hist)
            else:
                bins_1 = shared_bins
                counts_1 = np.zeros(len(shared_bins) - 1, dtype=int)
                kde_1 = None
            if len(data_2_hist) > 0:
                bins_2, counts_2 = self._build_histogram(data_2_hist, shared_bins)
                kde_2 = self._build_kde(data_2_hist)
            else:
                bins_2 = shared_bins
                counts_2 = np.zeros(len(shared_bins) - 1, dtype=int)
                kde_2 = None

        # Compute normalized counts
        total_1 = counts_1.sum() if len(counts_1) > 0 else 0
        total_2 = counts_2.sum() if len(counts_2) > 0 else 0
        counts_normalized_1 = (
            (counts_1 / total_1).tolist() if total_1 > 0 else counts_1.tolist()
        )
        counts_normalized_2 = (
            (counts_2 / total_2).tolist() if total_2 > 0 else counts_2.tolist()
        )

        # Compute Wasserstein distance (on transformed data if log scale)
        if len(data_1_hist) > 0 and len(data_2_hist) > 0:
            distance = float(wasserstein_distance(data_1_hist, data_2_hist))
        else:
            distance = float("nan")

        return {
            "distribution_1": {
                "bins": bins_1.tolist(),
                "counts": counts_1.tolist(),
                "counts_normalized": counts_normalized_1,
                "kde": kde_1,
                "mean": float(np.mean(data_1_hist))
                if len(data_1_hist) > 0
                else float("nan"),
                "median": float(np.median(data_1_hist))
                if len(data_1_hist) > 0
                else float("nan"),
            },
            "distribution_2": {
                "bins": bins_2.tolist(),
                "counts": counts_2.tolist(),
                "counts_normalized": counts_normalized_2,
                "kde": kde_2,
                "mean": float(np.mean(data_2_hist))
                if len(data_2_hist) > 0
                else float("nan"),
                "median": float(np.median(data_2_hist))
                if len(data_2_hist) > 0
                else float("nan"),
            },
            "distance": distance,
            "log_scale": log_scale,
        }
