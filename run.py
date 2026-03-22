"""
DataPulse Main Runner.
Usage: python run.py
"""
import glob
from engine.profiler import profile_source, print_profile
from engine.check_runner import run_all_checks, print_results, load_checks
from engine.storage import init_db, save_check_results, save_profile
from engine.alerts import alert_on_failures

def main():
    print("\n--- DataPulse Starting ---\n")

    init_db()

    check_files = glob.glob("checks/*.yaml")
    if not check_files:
        print("No check files found in checks/ folder!")
        return

    print(f"Found {len(check_files)} check file(s)\n")

    total_passed = 0
    total_failed = 0

    for check_file in check_files:
        config = load_checks(check_file)
        source = config["source"]

        # Profile
        profile = profile_source(source)
        print_profile(profile)
        save_profile(source, profile)

        # Check
        results = run_all_checks(check_file)
        print_results(results, source)
        save_check_results(results, source)

        alert_on_failures(results, source)

        total_passed += sum(1 for r in results if r["passed"])
        total_failed += sum(1 for r in results if not r["passed"])

    print(f"\n--- DONE: {total_passed} passed, {total_failed} failed ---\n")


if __name__ == "__main__":
    main()