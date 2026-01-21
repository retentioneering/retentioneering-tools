import sys
import warnings

from retentioneering import __version__

python_version = sys.version_info[0:3]

unsupported_python_version = [(3, 9, 7)]

min_python_version = (3, 9, 0)
max_python_version = (3, 12, 100)


if (
    python_version < min_python_version
    or python_version in unsupported_python_version
    or python_version > max_python_version
):
    supported_python_version = ", ".join(["3.9.* (except 3.9.7)", "3.10.*", "3.11.*", "3.12.*"])
    warnings.warn(
        f"For retentioneering version {__version__}, the following Python versions are supported: {supported_python_version}"
    )
