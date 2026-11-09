#!/usr/bin/env python3
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def parse_fields(line: str) -> dict[str, str]:
    marker = "timestamp="
    start = line.find(marker)
    if start < 0:
        return {}
    fields = {}
    for token in line[start:].strip().split():
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    return fields


def percentile(values: list[int], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def main() -> int:
    if len(sys.argv) != 2:
        print(f"uso: {sys.argv[0]} CAMINHO_DO_LOG", file=sys.stderr)
        return 2

    events = [parse_fields(line) for line in Path(sys.argv[1]).read_text().splitlines()]
    events = [event for event in events if event]
    client_commits = [
        event for event in events
        if event.get("event") == "COMMITTED" and event.get("component", "").startswith("client-")
    ]
    cluster_events = [event for event in events if event.get("component", "").startswith("cluster-")]
    cluster_commits = [event for event in cluster_events if event.get("event") == "COMMITTED"]
    waits = [int(event["wait_ns"]) for event in client_commits]
    by_client = Counter(event["component"] for event in client_commits)
    observer_events = [event for event in events if event.get("component") == "observer"]
    starts = [datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")) for event in events
              if event.get("event") == "START" and event.get("component", "").startswith("client-")]
    summaries = [datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")) for event in events
                 if event.get("event") == "SUMMARY" and event.get("component", "").startswith("client-")]
    elapsed_seconds = (max(summaries) - min(starts)).total_seconds() if starts and summaries else 0.0

    max_concurrent = max((int(event.get("max_concurrent", 0)) for event in observer_events), default=0)
    overlaps = max((int(event.get("overlap_count", 0)) for event in observer_events), default=0)
    ready_events = [event for event in cluster_events if event.get("event") == "READY"]
    protected = all(event.get("lock_enabled") == "true" for event in ready_events)
    barrier_enabled = all(event.get("barrier_enabled") == "true" for event in ready_events)
    expected_requests = sum(int(event["requests"]) for event in events
                            if event.get("event") == "START"
                            and event.get("component", "").startswith("client-"))

    checks = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    check(
        "progresso",
        expected_requests > 0
        and expected_requests == len(client_commits) == len(cluster_commits),
        f"planejados={expected_requests}, cluster_committed={len(cluster_commits)}, "
        f"clientes_confirmaram={len(client_commits)}",
    )

    lifecycle = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]
    request_events = defaultdict(list)
    for event in cluster_events:
        if event.get("request_id") and event.get("event") in lifecycle:
            request_events[event["request_id"]].append(event["event"])
    invalid_lifecycles = [
        request_id for request_id, sequence in request_events.items() if sequence != lifecycle
    ]
    check(
        "ciclo de vida",
        len(request_events) == expected_requests and not invalid_lifecycles,
        "REQUEST→ACQUIRE→ENTER→EXIT→RELEASE→COMMITTED em todos os pedidos"
        if not invalid_lifecycles else f"pedidos inválidos={invalid_lifecycles[:5]}",
    )

    begins = Counter(event.get("request_id") for event in observer_events if event.get("event") == "BEGIN")
    ends = Counter(event.get("request_id") for event in observer_events if event.get("event") == "END")
    check(
        "pares BEGIN/END",
        begins == ends and sum(begins.values()) == expected_requests,
        f"BEGIN={sum(begins.values())}, END={sum(ends.values())}",
    )

    if protected:
        check(
            "exclusão mútua",
            max_concurrent <= 1 and overlaps == 0,
            f"máximo_simultâneo={max_concurrent}, sobreposições={overlaps}",
        )
    else:
        check(
            "detecção sem mutex",
            max_concurrent > 1 and overlaps > 0,
            f"máximo_simultâneo={max_concurrent}, sobreposições={overlaps}",
        )

    if barrier_enabled:
        entered_by_round = Counter(event.get("round") for event in cluster_events
                                   if event.get("event") == "BARRIER_ENTERED")
        left_by_round = Counter(event.get("round") for event in cluster_events
                                if event.get("event") == "BARRIER_LEFT")
        participant_count = len(ready_events)
        barrier_ok = (
            bool(entered_by_round)
            and entered_by_round == left_by_round
            and all(count == participant_count for count in entered_by_round.values())
        )
        check(
            "barreira dupla por rodada",
            barrier_ok,
            f"rodadas={len(entered_by_round)}, participantes_por_rodada={participant_count}",
        )

    approved = all(item["passed"] for item in checks)
    report = {
        "approved": approved,
        "checks": checks,
        "configuration": {
            "lock_enabled": protected,
            "barrier_enabled": barrier_enabled,
            "cluster_processes": len(ready_events),
        },
        "completed_requests": len(client_commits),
        "completed_by_client": dict(sorted(by_client.items())),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "throughput_requests_per_second": (
            round(len(client_commits) / elapsed_seconds, 3) if elapsed_seconds else 0.0
        ),
        "acquisition_wait_ms": {
            "median": round(statistics.median(waits) / 1_000_000, 3) if waits else 0.0,
            "p95": round(percentile(waits, 0.95) / 1_000_000, 3),
            "maximum": round(max(waits) / 1_000_000, 3) if waits else 0.0,
        },
        "audit": {
            "maximum_concurrent_accesses": max_concurrent,
            "overlap_violations": overlaps,
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if approved else 1


if __name__ == "__main__":
    raise SystemExit(main())

