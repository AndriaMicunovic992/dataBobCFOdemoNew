"""Cross-platform helper: wait until PostgreSQL is ready."""
import subprocess
import sys
import time

print("Waiting for PostgreSQL", end="", flush=True)

for _ in range(30):
    result = subprocess.run(
        [
            "docker-compose", "exec", "-T", "postgres",
            "pg_isready", "-U", "databobiq", "-d", "databobiq",
        ],
        capture_output=True,
    )
    if result.returncode == 0:
        print(" ready.")
        sys.exit(0)
    print(".", end="", flush=True)
    time.sleep(1)

print(" timed out!")
sys.exit(1)
