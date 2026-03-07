#!/usr/bin/env python3
"""PyO3-backed backend soak benchmark with running statistics and summary reporting.

Examples:
  python3 scripts/python_backend_benchmark.py --backend postgres --pg-units 500000 --up --down
  python3 scripts/python_backend_benchmark.py --backend neo4j --neo4j-units 1000 --up --down
  python3 scripts/python_backend_benchmark.py --backend all --up --down
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import platform
import socket
import subprocess
import sys
import time
import uuid
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_IPTO_MODULE = None


DEFAULT_ENV = {
    "IPTO_PG_HOST": "localhost",
    "IPTO_PG_PORT": "5432",
    "IPTO_PG_USER": "repo",
    "IPTO_PG_PASSWORD": "repo",
    "IPTO_PG_DATABASE": "repo",
    "IPTO_NEO4J_HTTP_PORT": "7474",
    "IPTO_NEO4J_BOLT_PORT": "7687",
    "IPTO_NEO4J_DATABASE": "neo4j",
    "IPTO_NEO4J_USER": "neo4j",
    "IPTO_NEO4J_PASSWORD": "neo4jtest",
}


@dataclass
class RunningStatistics:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    total: float = 0.0
    min_value: float = math.nan
    max_value: float = math.nan

    def add_sample(self, value: float) -> None:
        if self.count == 0:
            self.min_value = value
            self.max_value = value
        else:
            self.min_value = min(self.min_value, value)
            self.max_value = max(self.max_value, value)

        self.total += value
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return math.nan
        return self.m2 / (self.count - 1)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance) if not math.isnan(self.variance) else math.nan

    @property
    def cv_percent(self) -> float:
        if self.count < 2 or self.mean == 0.0:
            return math.nan
        return 100.0 * self.stddev / self.mean


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PyO3-backed IPTO backend soak benchmark with running statistics"
    )
    parser.add_argument(
        "--backend",
        choices=("postgres", "neo4j", "all"),
        default="all",
        help="Backend to benchmark (default: all)",
    )
    parser.add_argument(
        "--pg-units",
        type=int,
        default=10_000,
        help="Units to store for PostgreSQL runs (default: 10000)",
    )
    parser.add_argument(
        "--neo4j-units",
        type=int,
        default=1_000,
        help="Units to store for Neo4j runs (default: 1000)",
    )
    parser.add_argument(
        "--tenant-id",
        type=int,
        default=1,
        help="Tenant ID used in payloads (default: 1)",
    )
    parser.add_argument(
        "--report-every",
        type=int,
        default=0,
        help="Progress report interval. 0 => backend-specific defaults.",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional explicit unit-name prefix. Defaults to timestamped unique prefix.",
    )
    parser.add_argument(
        "--up",
        action="store_true",
        help="Run `docker compose up -d` for selected backend(s) before benchmark",
    )
    parser.add_argument(
        "--down",
        action="store_true",
        help="Run `docker compose down` after benchmark",
    )
    parser.add_argument(
        "--down-volumes",
        action="store_true",
        help="Run `docker compose down -v` after benchmark",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue benchmark loop after store failures (default: stop on first failure)",
    )
    parser.add_argument(
        "--skip-verify-count",
        action="store_true",
        help="Skip post-run count verification by searching for benchmark name prefix",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes per backend (default: 1)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries for transient store conflicts (deadlocks/serialization). default: 3",
    )
    return parser.parse_args()


def apply_env_defaults() -> None:
    for key, value in DEFAULT_ENV.items():
        os.environ.setdefault(key, value)
    os.environ.setdefault(
        "IPTO_NEO4J_URL", f"http://localhost:{os.environ['IPTO_NEO4J_HTTP_PORT']}"
    )
    os.environ.setdefault(
        "IPTO_NEO4J_AUTH",
        f"{os.environ['IPTO_NEO4J_USER']}/{os.environ['IPTO_NEO4J_PASSWORD']}",
    )


def docker_compose(*args: str) -> None:
    cmd = ["docker", "compose", *args]
    subprocess.run(cmd, check=True)


def wait_for_port(host: str, port: int, timeout_seconds: int = 180) -> None:
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"timed out waiting for {host}:{port}")
            time.sleep(1)


def maybe_up(backends: list[str]) -> None:
    services = []
    if "postgres" in backends:
        services.append("postgres")
    if "neo4j" in backends:
        services.append("neo4j")
    if not services:
        return
    docker_compose("up", "-d", *services)
    if "postgres" in backends:
        wait_for_port(os.environ["IPTO_PG_HOST"], int(os.environ["IPTO_PG_PORT"]))
        wait_for_postgres_ready()
    if "neo4j" in backends:
        wait_for_port("localhost", int(os.environ["IPTO_NEO4J_BOLT_PORT"]))
        wait_for_neo4j_ready()


def wait_for_postgres_ready(timeout_seconds: int = 180) -> None:
    started = time.time()
    while True:
        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "pg_isready",
            "-U",
            os.environ["IPTO_PG_USER"],
            "-d",
            os.environ["IPTO_PG_DATABASE"],
        ]
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
        if time.time() - started > timeout_seconds:
            raise TimeoutError("PostgreSQL did not become ready in time")
        time.sleep(2)


def wait_for_neo4j_ready(timeout_seconds: int = 240) -> None:
    started = time.time()
    while True:
        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "neo4j",
            "cypher-shell",
            "-u",
            os.environ["IPTO_NEO4J_USER"],
            "-p",
            os.environ["IPTO_NEO4J_PASSWORD"],
            "RETURN 1;",
        ]
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
        if time.time() - started > timeout_seconds:
            raise TimeoutError("Neo4j did not become ready in time")
        time.sleep(2)


def maybe_down(down_volumes: bool) -> None:
    if down_volumes:
        docker_compose("down", "-v")
    else:
        docker_compose("down")


def ensure_pg_tenant(tenant_id: int) -> None:
    tenant_name = f"py_bench_tenant_{tenant_id}"
    sql = (
        "INSERT INTO repo.repo_tenant (tenantid, name, description) "
        f"VALUES ({tenant_id}, '{tenant_name}', 'python benchmark tenant') "
        "ON CONFLICT (tenantid) DO NOTHING;"
    )
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        os.environ["IPTO_PG_USER"],
        "-d",
        os.environ["IPTO_PG_DATABASE"],
        "-c",
        sql,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def build_payload(tenant_id: int, unit_name: str) -> str:
    return json.dumps(
        {
            "tenantid": tenant_id,
            "corrid": str(uuid.uuid4()),
            "status": 30,
            "unitname": unit_name,
            "attributes": [],
        }
    )


def pyo3_client(backend: str) -> Any:
    global _IPTO_MODULE
    try:
        if _IPTO_MODULE is None:
            import ipto_rust

            _IPTO_MODULE = ipto_rust
    except Exception as exc:  # pragma: no cover
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        venv_python = os.path.join(root_dir, ".venv", "bin", "python")
        if (
            os.path.exists(venv_python)
            and os.path.realpath(sys.executable) != os.path.realpath(venv_python)
            and os.environ.get("IPTO_BENCH_REEXECED", "") != "1"
        ):
            env = os.environ.copy()
            env["IPTO_BENCH_REEXECED"] = "1"
            os.execve(venv_python, [venv_python, *sys.argv], env)
        raise RuntimeError(
            "Failed to import ipto_rust. Run `maturin develop` in the Python environment you use "
            f"(current interpreter: {sys.executable})."
        ) from exc
    return _IPTO_MODULE.PyIpto(backend)


def print_header(backends: list[str], args: argparse.Namespace) -> None:
    print("=" * 100)
    print("IPTO PyO3 backend benchmark")
    print(
        f"Started: {datetime.now(timezone.utc).isoformat()} | "
        f"Python {platform.python_version()} | OS {platform.system()} {platform.release()} | "
        f"CPU cores {os.cpu_count()}"
    )
    print(
        f"Backends={backends} tenant={args.tenant_id} pg_units={args.pg_units} neo4j_units={args.neo4j_units}"
    )
    print(
        "PG env: host={host} port={port} db={db} user={user}".format(
            host=os.environ["IPTO_PG_HOST"],
            port=os.environ["IPTO_PG_PORT"],
            db=os.environ["IPTO_PG_DATABASE"],
            user=os.environ["IPTO_PG_USER"],
        )
    )
    print(
        "Neo4j env: url={url} bolt_port={bolt} db={db} user={user}".format(
            url=os.environ["IPTO_NEO4J_URL"],
            bolt=os.environ["IPTO_NEO4J_BOLT_PORT"],
            db=os.environ["IPTO_NEO4J_DATABASE"],
            user=os.environ["IPTO_NEO4J_USER"],
        )
    )
    print("=" * 100, flush=True)


def verify_count(client: Any, tenant_id: int, unit_prefix: str) -> int:
    expression = json.dumps({"tenantid": tenant_id, "name_ilike": f"{unit_prefix}%"})
    raw = client.search_units(expression, "created", True, 1, 0)
    result = json.loads(raw)
    return int(result.get("total_hits", 0))


def run_store_benchmark(
    backend: str,
    count: int,
    tenant_id: int,
    prefix: str,
    report_every: int,
    continue_on_error: bool,
    skip_verify_count: bool,
    workers: int,
    max_retries: int,
) -> dict[str, Any]:
    report_every = report_every or (10_000 if backend == "postgres" else 100)
    worker_count = min(max(workers, 1), count) if count > 0 else 1
    if worker_count == 1:
        local = run_store_worker(
            backend=backend,
            tenant_id=tenant_id,
            prefix=prefix,
            start_idx=1,
            end_idx=count,
            report_every=report_every,
            continue_on_error=continue_on_error,
            quiet=False,
            max_retries=max_retries,
        )
    else:
        print(f"[{backend}] using workers={worker_count}", flush=True)
        local = run_store_parallel(
            backend=backend,
            tenant_id=tenant_id,
            prefix=prefix,
            count=count,
            report_every=report_every,
            continue_on_error=continue_on_error,
            workers=worker_count,
            max_retries=max_retries,
        )

    wall_seconds = float(local["elapsed_s"])
    ok_count = int(local["ok"])
    throughput = ok_count / wall_seconds if wall_seconds > 0 else 0.0
    mean_ms = float(local["lat_mean_ms"])
    min_ms = float(local["lat_min_ms"]) if local["lat_min_ms"] is not None else math.nan
    stddev_ms = float(local["lat_stddev_ms"]) if local["lat_stddev_ms"] is not None else math.nan
    p95_text = "n/a"
    if ok_count > 1 and not math.isnan(stddev_ms):
        # Approximate p95 under normality assumption as a lightweight indicator.
        p95 = mean_ms + 1.645 * stddev_ms
        p95_text = f"{max(p95, min_ms):.3f}"

    matched = None
    if not skip_verify_count:
        client = pyo3_client(backend)
        matched = verify_count(client, tenant_id, f"{prefix}-{backend}-")

    return {
        "backend": backend,
        "requested": count,
        "workers": worker_count,
        "ok": ok_count,
        "errors": int(local["errors"]),
        "elapsed_s": wall_seconds,
        "throughput_ops_s": throughput,
        "lat_min_ms": local["lat_min_ms"],
        "lat_max_ms": local["lat_max_ms"],
        "lat_mean_ms": local["lat_mean_ms"],
        "lat_stddev_ms": local["lat_stddev_ms"],
        "lat_cv_percent": local["lat_cv_percent"],
        "lat_total_ms": local["lat_total_ms"],
        "lat_p95_est_ms": p95_text,
        "first_unit_id": local["first_unit_id"],
        "last_unit_id": local["last_unit_id"],
        "matched_by_name_prefix": matched,
        "api_stats": local["api_stats"],
    }

def _split_ranges(count: int, workers: int) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    base = count // workers
    remainder = count % workers
    start = 1
    for i in range(workers):
        size = base + (1 if i < remainder else 0)
        end = start + size - 1
        ranges.append((start, end))
        start = end + 1
    return [r for r in ranges if r[0] <= r[1]]


def merge_api_stats(stats_list: list[dict[str, Any]], backend: str) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}
    for stats in stats_list:
        for op_name, op in stats.get("operations", {}).items():
            target = merged.setdefault(
                op_name,
                {
                    "count": 0,
                    "errors": 0,
                    "total_ms": 0.0,
                    "min_ms": None,
                    "max_ms": None,
                },
            )
            count = int(op.get("count", 0))
            errors = int(op.get("errors", 0))
            total_ms = float(op.get("total_ms", 0.0))
            min_ms = op.get("min_ms")
            max_ms = op.get("max_ms")

            target["count"] += count
            target["errors"] += errors
            target["total_ms"] += total_ms
            if min_ms is not None:
                target["min_ms"] = min_ms if target["min_ms"] is None else min(target["min_ms"], min_ms)
            if max_ms is not None:
                target["max_ms"] = max_ms if target["max_ms"] is None else max(target["max_ms"], max_ms)

    operations: dict[str, dict[str, Any]] = {}
    for op_name, op in merged.items():
        count = op["count"]
        mean_ms = op["total_ms"] / count if count > 0 else 0.0
        operations[op_name] = {
            "count": count,
            "errors": op["errors"],
            "total_ms": op["total_ms"],
            "min_ms": op["min_ms"],
            "max_ms": op["max_ms"],
            "mean_ms": mean_ms,
        }
    return {"backend": backend, "operations": operations}


def merge_worker_results(results: list[dict[str, Any]], backend: str) -> dict[str, Any]:
    stats = RunningStatistics()
    for result in results:
        count = int(result["ok"])
        mean = float(result["lat_mean_ms"])
        m2 = float(result["m2"])
        total = float(result["lat_total_ms"])
        min_value = result["lat_min_ms"]
        max_value = result["lat_max_ms"]

        if stats.count == 0:
            stats.count = count
            stats.mean = mean
            stats.m2 = m2
            stats.total = total
            stats.min_value = min_value if min_value is not None else math.nan
            stats.max_value = max_value if max_value is not None else math.nan
            continue

        new_count = stats.count + count
        if new_count == 0:
            continue
        delta = mean - stats.mean
        stats.mean = (stats.mean * stats.count + mean * count) / new_count
        stats.m2 = stats.m2 + m2 + delta * delta * stats.count * count / new_count
        stats.count = new_count
        stats.total += total
        if min_value is not None:
            stats.min_value = min(min_value, stats.min_value) if not math.isnan(stats.min_value) else min_value
        if max_value is not None:
            stats.max_value = max(max_value, stats.max_value) if not math.isnan(stats.max_value) else max_value

    all_first = [r["first_unit_id"] for r in results if r["first_unit_id"] is not None]
    all_last = [r["last_unit_id"] for r in results if r["last_unit_id"] is not None]
    return {
        "ok": stats.count,
        "errors": sum(int(r["errors"]) for r in results),
        "elapsed_s": max(float(r["elapsed_s"]) for r in results) if results else 0.0,
        "lat_min_ms": stats.min_value if stats.count > 0 else None,
        "lat_max_ms": stats.max_value if stats.count > 0 else None,
        "lat_mean_ms": stats.mean if stats.count > 0 else 0.0,
        "lat_stddev_ms": stats.stddev if stats.count > 1 else math.nan,
        "lat_cv_percent": stats.cv_percent if stats.count > 1 else math.nan,
        "lat_total_ms": stats.total,
        "m2": stats.m2,
        "first_unit_id": min(all_first) if all_first else None,
        "last_unit_id": max(all_last) if all_last else None,
        "api_stats": merge_api_stats([r["api_stats"] for r in results], backend),
    }


def run_store_parallel(
    backend: str,
    tenant_id: int,
    prefix: str,
    count: int,
    report_every: int,
    continue_on_error: bool,
    workers: int,
    max_retries: int,
) -> dict[str, Any]:
    tasks = _split_ranges(count, workers)
    results: list[dict[str, Any]] = []
    previous_bootstrap = os.environ.get("IPTO_NEO4J_BOOTSTRAP")
    if backend == "neo4j":
        # Avoid bootstrap race when many workers create clients simultaneously.
        warmup = pyo3_client("neo4j")
        _ = warmup.health()
        os.environ["IPTO_NEO4J_BOOTSTRAP"] = "false"
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                run_store_worker,
                backend,
                tenant_id,
                prefix,
                start_idx,
                end_idx,
                report_every,
                continue_on_error,
                True,
                max_retries,
            )
            for start_idx, end_idx in tasks
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"[{backend}] worker_done ok={result['ok']} err={result['errors']} "
                f"range={result['start_idx']}..{result['end_idx']}",
                flush=True,
            )
    if backend == "neo4j":
        if previous_bootstrap is None:
            os.environ.pop("IPTO_NEO4J_BOOTSTRAP", None)
        else:
            os.environ["IPTO_NEO4J_BOOTSTRAP"] = previous_bootstrap
    return merge_worker_results(results, backend)


def run_store_worker(
    backend: str,
    tenant_id: int,
    prefix: str,
    start_idx: int,
    end_idx: int,
    report_every: int,
    continue_on_error: bool,
    quiet: bool,
    max_retries: int,
) -> dict[str, Any]:
    client = pyo3_client(backend)
    client.reset_statistics()
    client.flush_cache()

    stats = RunningStatistics()
    errors = 0
    first_unit_id = None
    last_unit_id = None
    local_count = end_idx - start_idx + 1
    wall_start = time.perf_counter()

    for i, idx in enumerate(range(start_idx, end_idx + 1), start=1):
        unit_name = f"{prefix}-{backend}-{idx:08d}"
        payload = build_payload(tenant_id, unit_name)

        started = time.perf_counter_ns()
        try:
            stored = store_with_retry(client, payload, max_retries)
            unit_id = stored.get("unitid")
            if first_unit_id is None:
                first_unit_id = unit_id
            last_unit_id = unit_id
        except Exception:
            errors += 1
            if not continue_on_error:
                raise
            continue

        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        stats.add_sample(elapsed_ms)
        if not quiet and (i % report_every == 0 or i == local_count):
            elapsed = time.perf_counter() - wall_start
            throughput = stats.count / elapsed if elapsed > 0 else 0.0
            eta = (local_count - i) / throughput if throughput > 0 else math.inf
            eta_text = f"{eta:.1f}s" if math.isfinite(eta) else "n/a"
            print(
                f"[{backend}] progress={idx}/{end_idx} ({(i / local_count) * 100:.1f}%) "
                f"ok={stats.count} err={errors} mean={stats.mean:.3f}ms "
                f"throughput={throughput:.1f} ops/s eta={eta_text}",
                flush=True,
            )

    elapsed_s = time.perf_counter() - wall_start
    if hasattr(client, "get_statistics"):
        api_stats = client.get_statistics()
    else:
        api_stats = json.loads(client.get_statistics_json())

    return {
        "start_idx": start_idx,
        "end_idx": end_idx,
        "ok": stats.count,
        "errors": errors,
        "elapsed_s": elapsed_s,
        "lat_min_ms": stats.min_value if stats.count > 0 else None,
        "lat_max_ms": stats.max_value if stats.count > 0 else None,
        "lat_mean_ms": stats.mean if stats.count > 0 else 0.0,
        "lat_stddev_ms": stats.stddev if stats.count > 1 else math.nan,
        "lat_cv_percent": stats.cv_percent if stats.count > 1 else math.nan,
        "lat_total_ms": stats.total,
        "m2": stats.m2,
        "first_unit_id": first_unit_id,
        "last_unit_id": last_unit_id,
        "api_stats": api_stats,
    }


def is_transient_store_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    transient_markers = (
        "deadlock",
        "serialization",
        "transienterror",
        "temporarily unavailable",
        "could not serialize",
    )
    return any(marker in msg for marker in transient_markers)


def store_with_retry(client: Any, payload: str, max_retries: int) -> dict[str, Any]:
    attempts = 0
    while True:
        try:
            return json.loads(client.store_unit_json(payload))
        except Exception as exc:
            attempts += 1
            if attempts > max_retries or not is_transient_store_error(exc):
                raise
            delay = min(0.5, 0.01 * (2 ** (attempts - 1))) + random.uniform(0.0, 0.01)
            time.sleep(delay)


def print_summary(results: list[dict[str, Any]]) -> None:
    print("\n" + "=" * 100)
    print("Benchmark summary")
    print("=" * 100)
    for result in results:
        print(f"backend={result['backend']}")
        print(
            "  requested={requested} workers={workers} ok={ok} errors={errors} elapsed={elapsed_s:.2f}s throughput={throughput_ops_s:.2f} ops/s".format(
                **result
            )
        )
        print(
            "  latency_ms: min={lat_min_ms:.3f} mean={lat_mean_ms:.3f} max={lat_max_ms:.3f} stddev={lat_stddev_ms:.3f} cv={lat_cv_percent:.2f}% p95_est={lat_p95_est_ms}".format(
                **result
            )
        )
        print(
            "  first_unit_id={first_unit_id} last_unit_id={last_unit_id} matched_by_name_prefix={matched_by_name_prefix}".format(
                **result
            )
        )
        print("  api_stats:")
        operations = result.get("api_stats", {}).get("operations", {})
        for op_name in sorted(operations.keys()):
            op = operations[op_name]
            print(
                "    {name}: count={count} errors={errors} mean={mean_ms:.3f}ms min={min_ms} max={max_ms}".format(
                    name=op_name,
                    count=op.get("count", 0),
                    errors=op.get("errors", 0),
                    mean_ms=op.get("mean_ms", 0.0),
                    min_ms=f"{op.get('min_ms', 0.0):.3f}ms"
                    if op.get("min_ms") is not None
                    else "n/a",
                    max_ms=f"{op.get('max_ms', 0.0):.3f}ms"
                    if op.get("max_ms") is not None
                    else "n/a",
                )
            )
    total_elapsed = sum(result["elapsed_s"] for result in results)
    total_ok = sum(result["ok"] for result in results)
    total_errors = sum(result["errors"] for result in results)
    total_throughput = total_ok / total_elapsed if total_elapsed > 0 else 0.0
    if len(results) > 1:
        print(
            f"TOTAL: ok={total_ok} errors={total_errors} elapsed={total_elapsed:.2f}s "
            f"aggregate_throughput={total_throughput:.2f} ops/s"
        )
    print("=" * 100, flush=True)


def main() -> int:
    args = parse_args()
    apply_env_defaults()

    backends = (
        ["postgres", "neo4j"]
        if args.backend == "all"
        else [args.backend]
    )

    base_prefix = args.prefix or datetime.now(timezone.utc).strftime("py-soak-%Y%m%d-%H%M%S")

    try:
        # Resolve PyO3 module/interpreter early so we do not start containers before a re-exec.
        _ = pyo3_client("postgres" if "postgres" in backends else "neo4j")
        print_header(backends, args)

        if args.up:
            maybe_up(backends)
            if "postgres" in backends:
                ensure_pg_tenant(args.tenant_id)

        results = []
        for backend in backends:
            count = args.pg_units if backend == "postgres" else args.neo4j_units
            print(f"\nStarting backend={backend} count={count} prefix={base_prefix}-{backend}-*", flush=True)
            result = run_store_benchmark(
                backend=backend,
                count=count,
                tenant_id=args.tenant_id,
                prefix=base_prefix,
                report_every=args.report_every,
                continue_on_error=args.continue_on_error,
                skip_verify_count=args.skip_verify_count,
                workers=args.workers,
                max_retries=args.max_retries,
            )
            results.append(result)
        print_summary(results)
    finally:
        if args.down or args.down_volumes:
            maybe_down(args.down_volumes)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
