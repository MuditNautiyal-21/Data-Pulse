"""
Data-Pulse Check Runner: reads YAML rules, tests data, reports results.
"""

import yaml
import pandas as pd
from datetime import datetime, date

def load_checks(yaml_path):
    """Read a YAML check file."""
    with open(yaml_path) as f:
        return yaml.safe_load(f)

def run_null_check(df, check):
    """To check: Are there any empty/missing values in this column?"""
    col = check["column"]
    nulls = int(df[col].isnull().sum())
    return {
        "passed": nulls == 0,
        "details": f"{nulls} nulls found in '{col}'"
    }

def run_unique_check(df, check):
    """To check: Are there any duplicate values?"""
    col = check["column"]
    dupes = len(df) - df[col].nunique()
    return {
        "passed": dupes == 0,
        "details": f"{dupes} duplicates found in '{col}'"
    }

def run_value_check(df, check):
    """ To check: Do all values satisfy a condition like '> 0'?"""
    col = check["column"]
    parts = check["condition"].split()
    op, val = parts[0], float(parts[1])

    data = df[col].dropna()
    if op == ">":
        bad = data[data <= val]
    elif op == "<":
        bad = data[data >= val]
    elif op == ">=":
        bad = data[data < val]
    elif op == "<=":
        bad = data[data > val]
    else:
        return {"passed": False, "details": f"Unknown operator: {op}"}
    
    return {
        "passed": len(bad) == 0,
        "details": f"{len(bad)} rows fail '{col} {check['condition']}'"
    }

def run_accepted_values_check(df, check):
    """To check: Are all values from an approved list?"""
    col = check["column"]
    allowed = check["accepted_values"]
    data = df[col].dropna()
    bad = data[~data.isin(allowed)]
    example = bad.unique().tolist()[:3]
    return {
        "passed": len(bad) == 0,
        "details": f"{len(bad)} invalid values. Examples: {examples}"
        if len(bad) > 0
        else "All values valid",
    }

def run_freshness_check(df, check):
    """To check: Are any dates mention of the future?"""
    col = check["column"]
    dates = pd.to_datetime(df[col], errors = "coerce")
    future = dates[dates > pd.Timestamp(date.today())]
    return {
        "passed": len(future) == 0,
        "details": f"{len(future)} rows have future dates",
    }

def run_row_count_check(df, check):
    """To check: Does the table have enough rows?"""
    minimum = check["min_count"]
    actual = len(df)
    return {
        "passed": actual >= minimum,
        "details": f"Expected >= {minimum} rows, got {actual}",
    }

# Mapping each check type to its function
CHECK_MAP = {
    "null_check": run_null_check,
    "unique_check": run_unique_check,
    "value_check": run_value_check,
    "accepted_values_check": run_accepted_values_check,
    "freshness_check": run_freshness_check,
    "row_count_check": run_row_count_check,
}

def run_all_checks(yaml_path):
    """To run every check in a YAML file. This will return list of results."""
    config = load_checks(yaml_path)
    df = pd.read_csv(config["source"])
    results = []

    for check in config["checks"]:
        func = CHECK_MAP.get(check["type"])
        if not func:
            results.append({
                "name": check["name"],
                "passed": False,
                "details": f"Unknown check type: {check['type']}",
                "severity": check.get("severity", "warning"),
            })
            continue

        result = func(df, check)
        results.append({
            "name": check["name"],
            "type": check["type"],
            "severity": check.get("severity", "warning"),
            "passed": result["passed"],
            "details": result["details"],
            "ran_at": datetime.now().isoformat(),
        })

    return results

def print_results(results, source):
    """This will show check results in terminal."""
    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed

    print()
    print("=" * 55)
    print(f"  CHECK RESULTS: {source}")
    print(f"  Passed: {passed}  |  Failed: {failed}")
    print("=" * 55)

    for r in results:
        icon = "PASS" if r["passed"] else "FAIL"
        sev = r["severity"].upper()
        print(f"\n  [{icon}] [{sev}] {r['name']}")
        print(f"    {r['details']}")

    print()
    print("=" * 55)


if __name__ == "__main__":
    results = run_all_checks("checks/orders_checks.yaml")
    print_results(results, "orders.csv")

    results = run_all_checks("checks/customers_checks.yaml")
    print_results(results, "customers.csv")