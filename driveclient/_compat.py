"""
Optional dependency import guards.
"""


def require_pandas():
    """Import and return pandas, raising a helpful error if missing."""
    try:
        import pandas as pd
        return pd
    except ImportError:
        raise ImportError(
            "pandas is required for this operation. "
            "Install it with: pip install driveclient[pandas]"
        )
