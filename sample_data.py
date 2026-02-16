from __future__ import annotations

from pathlib import Path
import json
import random
from datetime import datetime, timedelta, timezone

MODULES = ["propulsion", "avionics", "guidance", "comms", "power", "thermal", "structures", "payload"]

def generate_runs(out: Path, runs: int = 10, tests_per_run: int = 80) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    created = []

    start = datetime.now(timezone.utc) - timedelta(days=runs)

    base_tests = []
    for i in range(tests_per_run):
        base_tests.append(
            {
                "test_name": f"TEST_{i:03d}",
                "module": random.choice(MODULES),
            }
        )

    # baseline failure seeds
    chronic_fail = set(random.sample([t["test_name"] for t in base_tests], k=max(3, tests_per_run // 25)))
    flaky = set(random.sample([t["test_name"] for t in base_tests], k=max(5, tests_per_run // 12)))

    for r in range(runs):
        run_id = f"RUN_{r+1:03d}"
        started_at = (start + timedelta(days=r)).isoformat()

        results = []
        for t in base_tests:
            name = t["test_name"]
            module = t["module"]

            # status logic
            status = "PASS"

            # chronic failures remain mostly failing
            if name in chronic_fail:
                status = "FAIL" if random.random() < 0.85 else "PASS"

            # flakies flip more often
            if name in flaky:
                status = "FAIL" if random.random() < 0.35 else "PASS"

            # occasionally introduce new regression later
            if r > runs // 2 and random.random() < 0.02:
                status = "FAIL"

            duration_s = max(0.2, random.gauss(1.4, 0.35))
            latency = max(1.0, random.gauss(22.0, 6.0))
            cpu = min(99.0, max(1.0, random.gauss(35.0, 8.0)))
            mem = max(50.0, random.gauss(420.0, 60.0))

            failure_code = None
            failure_log = None
            if status == "FAIL":
                failure_code = random.choice(["TIMEOUT", "CRC_MISMATCH", "SENSOR_OOR", "ASSERT", "BUS_ERROR"])
                failure_log = f"{module.upper()}::{failure_code} at step {random.randint(1, 40)}"

                # make regressed runs slightly worse on metrics
                latency += random.uniform(4.0, 12.0)
                cpu += random.uniform(1.0, 6.0)

            results.append(
                {
                    "test_name": name,
                    "module": module,
                    "status": status,
                    "duration_s": float(duration_s),
                    "metric_latency_ms": float(latency),
                    "metric_cpu_pct": float(cpu),
                    "metric_mem_mb": float(mem),
                    "failure_code": failure_code,
                    "failure_log": failure_log,
                }
            )

        payload = {
            "run": {
                "run_id": run_id,
                "started_at": started_at,
                "suite": "HIL_REGRESSION",
                "vehicle": "MOCK_VEHICLE_X",
                "build": f"build-{1000+r}",
                "notes": "",
            },
            "results": results,
        }

        path = out / f"{run_id}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        created.append(path)

    return created
