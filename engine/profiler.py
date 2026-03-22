"""
Data-Pulse Profiler: It is to look at your data and report what it sees.
"""

import pandas as pd
from datetime import datetime

from scipy import stats

def profile_source(file_path):
    """ Read a csv and return the stats about every column."""
    df = pd.read_csv(file_path)
    total_rows = len(df)

    profile = {
        "source": file_path,
        "profiled_at": datetime.now().isoformat(),
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "columns": {},
    }

    for col in df.columns:
        data = df[col]
        nulls = int(data.isnull().sum())

        stats = {
            "data_type": str(data.dtype),
            "null_count": nulls,
            "null_percent": round((nulls/total_rows) * 100, 2),
            "unique_count": int(data.nunique()),
        }

        # Numbers get min/max/average
        if data.dtype in ["int64", "float64"]:
            stats["min"] = round(float(data.min()), 2)
            stats["max"] = round(float(data.max()), 2)
            stats["average"] = round(float(data.mean()), 2)

        profile["columns"][col] = stats

    return profile


def print_profile(profile):
    """Show profile results in the terminal."""

    print()
    print("=" * 55)
    print(f"  PROFILE: {profile['source']}")
    print(f"  Rows: {profile['total_rows']}")
    print(f"  Columns: {profile['total_columns']}")
    print("=" * 55)
    
    for name, stats in profile["columns"].items():
        print(f"\n {name} ({stats['data_type']})")
        print(f"  Nulls: {stats['null_count']} ({stats['null_percent']}%)")
        print(f"  Unique Values: {stats['unique_count']}")
        if "min" in stats:
            print(f"  Min: {stats['min']} Max: {stats['max']} Avg: {stats['average']}")

    print()
    print("=" * 55)

# -- Running directly to test --
if __name__ == "__main__":
    p = profile_source("sample_data/orders.csv")
    print_profile(p)

    p = profile_source("sample_data/customers.csv")
    print_profile(p)
