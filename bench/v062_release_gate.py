#!/usr/bin/env python3
"""Evaluate the v0.6.2 five-round Redis 6.2.23 release matrix."""

import argparse
import csv
import json
import math
from pathlib import Path


WORKLOADS = ("string-mixed", "hash-mixed", "zset-mixed")
PIPELINES = (16, 64)


def geometric_mean(values):
    return math.exp(sum(math.log(value) for value in values) / len(values))


def load_rows(path: Path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def row_index(rows):
    return {
        (row["target"], row["policy"], row.get("scenario", "normal"),
         row["workload"], int(row["pipeline"])): row
        for row in rows
    }


def select_zset(rows, required_rounds: int):
    index = row_index(rows)
    scores = {}
    for target in ("skiplist", "rbtree"):
        qps = []
        for policy in ("off", "everysec"):
            for pipeline in PIPELINES:
                key = (target, policy, "normal", "zset-mixed", pipeline)
                row = index.get(key)
                if row is None or int(row["rounds"]) != required_rounds or \
                        int(row["cv_valid"]) != 1:
                    raise ValueError(f"missing or invalid ZSet selection row: {key}")
                qps.append(float(row["qps_median"]))
        scores[target] = geometric_mean(qps)
    relative = abs(scores["skiplist"] - scores["rbtree"]) / max(scores.values())
    winner = ("skiplist" if relative <= 0.01 else
              max(scores, key=scores.get))
    return winner, scores, relative


def evaluate(rows, required_rounds: int, expected_keyspace: int = 100_000):
    index = row_index(rows)
    winner, zset_scores, zset_relative = select_zset(rows, required_rounds)
    failures = []
    checks = []

    def project_target(workload):
        if workload == "string-mixed":
            return "string"
        if workload == "hash-mixed":
            return "hash"
        return winner

    def require(target, policy, scenario, workload, pipeline):
        key = (target, policy, scenario, workload, pipeline)
        row = index.get(key)
        if row is None:
            failures.append(f"missing row {key}")
            return None
        if int(row["rounds"]) != required_rounds:
            failures.append(f"{key}: rounds={row['rounds']} != {required_rounds}")
        if int(row["cv_valid"]) != 1:
            failures.append(f"{key}: QPS CV exceeds 5%")
        if int(row["errors_total"]) != 0:
            failures.append(f"{key}: errors_total={row['errors_total']}")
        if scenario == "bgsave" and int(
                row.get("rdb_recovery_cardinality", "0")) != expected_keyspace:
            failures.append(
                f"{key}: recovered RDB cardinality is not {expected_keyspace}")
        return row

    for workload in WORKLOADS:
        target = project_target(workload)
        for pipeline in PIPELINES:
            ordinary = {}
            for policy in ("off", "everysec"):
                project = require(target, policy, "normal", workload, pipeline)
                redis = require("redis", policy, "normal", workload, pipeline)
                if project is None or redis is None:
                    continue
                project_qps = float(project["qps_median"])
                redis_qps = float(redis["qps_median"])
                project_p99 = float(project["p99_us_median"])
                redis_p99 = float(redis["p99_us_median"])
                passed = project_qps >= redis_qps and project_p99 <= redis_p99
                checks.append({
                    "workload": workload, "pipeline": pipeline,
                    "policy": policy, "scenario": "normal", "passed": passed,
                    "project_qps": project_qps, "redis_qps": redis_qps,
                    "project_p99_us": project_p99,
                    "redis_p99_us": redis_p99,
                })
                if not passed:
                    failures.append(
                        f"{workload}/P{pipeline}/{policy}: project must have "
                        "QPS >= Redis and P99 <= Redis")
                ordinary[policy] = (project_qps, project_p99)
            if set(ordinary) == {"off", "everysec"}:
                off_qps, off_p99 = ordinary["off"]
                every_qps, every_p99 = ordinary["everysec"]
                qps_loss = (off_qps - every_qps) / off_qps
                p99_growth = (every_p99 - off_p99) / off_p99
                if qps_loss > 0.05:
                    failures.append(
                        f"{workload}/P{pipeline}: everysec QPS loss {qps_loss:.2%}")
                if p99_growth > 0.10:
                    failures.append(
                        f"{workload}/P{pipeline}: everysec P99 growth {p99_growth:.2%}")

            project = require(target, "everysec", "bgsave", workload, pipeline)
            redis = require("redis", "everysec", "bgsave", workload, pipeline)
            if project is not None and redis is not None:
                passed = (float(project["qps_median"]) >=
                          float(redis["qps_median"]) and
                          float(project["p99_us_median"]) <=
                          float(redis["p99_us_median"]))
                checks.append({
                    "workload": workload, "pipeline": pipeline,
                    "policy": "everysec", "scenario": "bgsave",
                    "passed": passed,
                    "project_qps": float(project["qps_median"]),
                    "redis_qps": float(redis["qps_median"]),
                    "project_p99_us": float(project["p99_us_median"]),
                    "redis_p99_us": float(redis["p99_us_median"]),
                })
                if not passed:
                    failures.append(
                        f"{workload}/P{pipeline}/BGSAVE: project must have "
                        "QPS >= Redis and P99 <= Redis")
    return {
        "passed": not failures,
        "zset_winner": winner,
        "zset_geomean_qps": zset_scores,
        "zset_relative_difference": zset_relative,
        "checks": checks,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-csv", required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keyspace", type=int, default=100_000)
    parser.add_argument("--json", help="optional machine-readable gate result")
    args = parser.parse_args()
    if args.rounds < 1 or args.keyspace < 1:
        parser.error("--rounds and --keyspace must be positive")
    try:
        result = evaluate(load_rows(Path(args.summary_csv)), args.rounds,
                          args.keyspace)
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    output = json.dumps(result, ensure_ascii=False, indent=2)
    print(output)
    if args.json:
        Path(args.json).write_text(output + "\n", encoding="utf-8")
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
