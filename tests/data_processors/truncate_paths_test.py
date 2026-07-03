import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


@pytest.fixture
def simple_eventstream():
    """Create a simple eventstream for testing."""
    data = pd.DataFrame({
        'user_id': [1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3],
        'event': ['A', 'B', 'C', 'D', 'E', 'A', 'B', 'C', 'D', 'X', 'B', 'C', 'Y', 'Z'],
        'timestamp': pd.to_datetime([
            '2023-01-01 10:00', '2023-01-01 11:00', '2023-01-01 12:00',
            '2023-01-01 13:00', '2023-01-01 14:00',
            '2023-01-02 10:00', '2023-01-02 11:00', '2023-01-02 12:00',
            '2023-01-02 13:00',
            '2023-01-03 10:00', '2023-01-03 11:00', '2023-01-03 12:00',
            '2023-01-03 13:00', '2023-01-03 14:00',
        ])
    })

    return Eventstream(data)


def test_truncate_paths_basic(simple_eventstream):
    """Test basic truncate_paths functionality."""
    result = simple_eventstream.truncate_paths(left='B', right='D')
    df = result.df

    # User 1: should have B, C, D
    user1_events = df[df['user_id'] == 1]['event'].tolist()
    assert user1_events == ['B', 'C', 'D']

    # User 2: should have B, C, D
    user2_events = df[df['user_id'] == 2]['event'].tolist()
    assert user2_events == ['B', 'C', 'D']

    # User 3: should have B, C (no D, so path should be filtered out)
    user3_events = df[df['user_id'] == 3]['event'].tolist()
    assert user3_events == []


def test_truncate_paths_same_boundary(simple_eventstream):
    """Test truncate_paths when left and right are the same event."""
    result = simple_eventstream.truncate_paths(left='C', right='C')
    df = result.df

    # Each path should have exactly one 'C' event
    assert len(df[df['user_id'] == 1]) == 1
    assert len(df[df['user_id'] == 2]) == 1
    assert len(df[df['user_id'] == 3]) == 1

    # All events should be 'C'
    assert all(df['event'] == 'C')


def test_truncate_paths_no_left_boundary(simple_eventstream):
    """Test truncate_paths when left boundary doesn't exist in some paths."""
    result = simple_eventstream.truncate_paths(left='X', right='Z')
    df = result.df

    # Only user 3 has both X and Z
    assert len(df[df['user_id'] == 1]) == 0
    assert len(df[df['user_id'] == 2]) == 0
    assert len(df[df['user_id'] == 3]) == 5  # X, B, C, Y, Z


def test_truncate_paths_no_right_boundary(simple_eventstream):
    """Test truncate_paths when right boundary doesn't exist in some paths."""
    result = simple_eventstream.truncate_paths(left='A', right='Z')
    df = result.df

    # Only users without Z should be filtered out
    # User 1: has A but no Z - filtered
    # User 2: has A but no Z - filtered
    # User 3: no A but has Z - filtered (no A)
    assert len(df[df['user_id'] == 1]) == 0
    assert len(df[df['user_id'] == 2]) == 0
    assert len(df[df['user_id'] == 3]) == 0


def test_truncate_paths_reverse_order():
    """Test truncate_paths when right boundary appears before left boundary."""
    data = pd.DataFrame({
        'user_id': [1, 1, 1, 1, 1],
        'event': ['D', 'C', 'B', 'A', 'E'],
        'timestamp': pd.to_datetime([
            '2023-01-01 10:00', '2023-01-01 11:00', '2023-01-01 12:00',
            '2023-01-01 13:00', '2023-01-01 14:00'
        ])
    })

    stream = Eventstream(data)
    result = stream.truncate_paths(left='B', right='D')
    df = result.df

    # Should be empty because 'D' appears before 'B' in the path
    assert len(df) == 0


def test_truncate_paths_multiple_occurrences():
    """Test truncate_paths with multiple occurrences of boundary events."""
    data = pd.DataFrame({
        'user_id': [1, 1, 1, 1, 1, 1, 1],
        'event': ['A', 'B', 'C', 'B', 'D', 'B', 'E'],
        'timestamp': pd.to_datetime([
            '2023-01-01 10:00', '2023-01-01 11:00', '2023-01-01 12:00',
            '2023-01-01 13:00', '2023-01-01 14:00', '2023-01-01 15:00',
            '2023-01-01 16:00'
        ])
    })

    stream = Eventstream(data)
    result = stream.truncate_paths(left='B', right='D')
    df = result.df

    # Should keep from first B (index 2) to first D after it (index 5)
    # Events: B, C, B, D
    events = df['event'].tolist()
    assert events == ['B', 'C', 'B', 'D']


def test_truncate_paths_empty_params():
    """Test truncate_paths with invalid parameters."""
    data = pd.DataFrame({
        'user_id': [1, 1, 1],
        'event': ['A', 'B', 'C'],
        'timestamp': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-01 12:00'])
    })

    stream = Eventstream(data)

    # Test with empty left parameter
    with pytest.raises(PreprocessingConfigError, match="Parameter 'left' must be a non-empty string"):
        stream.truncate_paths(left='', right='C')

    # Test with empty right parameter
    with pytest.raises(PreprocessingConfigError, match="Parameter 'right' must be a non-empty string"):
        stream.truncate_paths(left='A', right='')


def test_truncate_paths_custom_columns():
    """Test truncate_paths with custom path_id_col and event_col."""
    data = pd.DataFrame({
        'user_id': [1, 1, 1, 1, 1, 1],
        'session_id': [1, 1, 2, 2, 2, 2],
        'event': ['X', 'Y', 'Z', 'X', 'Y', 'Z'],
        'custom_event': ['A', 'B', 'A', 'B', 'C', 'A'],
        'timestamp': pd.to_datetime([
            '2023-01-01 10:00', '2023-01-01 11:00',
            '2023-01-01 12:00', '2023-01-01 13:00',
            '2023-01-01 14:00', '2023-01-01 15:00'
        ])
    })

    schema = {
        'path_cols': ['user_id', 'session_id'],
        'event_cols': ['event', 'custom_event']
    }

    stream = Eventstream(data, schema)
    result = stream.truncate_paths(
        left='B',
        right='A',
        path_id_col='session_id',
        event_col='custom_event'
    )
    df = result.df

    events = df['custom_event'].tolist()
    assert events == ['B', 'C', 'A']
