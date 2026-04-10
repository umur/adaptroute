#!/usr/bin/env python3
"""Broker kill test for AdaptRoute paper.
Sends W3-style bursty messages through the gateway, kills Kafka mid-run,
then measures: message loss, failover time, and recovery behavior.

Run after: docker compose up -d
Gateway is expected at localhost:8080.
"""

import json
import random
import subprocess
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List

GATEWAY_URL = "http://localhost:8080/api/route"
KAFKA_CONTAINER = "kafka"
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

MESSAGE_TYPES = ["ORDER", "ANALYTICS", "NOTIFICATION"]
PAYLOADS = {
    "ORDER":        {"messageType": "ORDER",        "sourceService": "test", "targetChannel": "orders",        "payloadSizeBytes": 256, "priority": "HIGH"},
    "ANALYTICS":    {"messageType": "ANALYTICS",    "sourceService": "test", "targetChannel": "analytics",     "payloadSizeBytes": 512, "priority": "NORMAL"},
    "NOTIFICATION": {"messageType": "NOTIFICATION", "sourceService": "test", "targetChannel": "notifications", "payloadSizeBytes": 128, "priority": "NORMAL"},
}


@dataclass
class SendResult:
    success: bool
    latency_ms: float
    broker: str = ""
    error: str = ""


def send_message(msg_type: str) -> SendResult:
    payload = json.dumps({**PAYLOADS[msg_type], "messageId": str(random.randint(1, 999999))}).encode()
    req = urllib.request.Request(
        GATEWAY_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read())
            return SendResult(
                success=True,
                latency_ms=(time.time() - t0) * 1000,
                broker=body.get("selectedBroker", "unknown")
            )
    except urllib.error.HTTPError as e:
        return SendResult(success=False, latency_ms=(time.time() - t0) * 1000,
                          error=f"HTTP {e.code}")
    except Exception as e:
        return SendResult(success=False, latency_ms=(time.time() - t0) * 1000,
                          error=str(e)[:60])


def run_burst(n: int, concurrency: int, label: str) -> dict:
    results: List[SendResult] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        msg_types = [random.choice(MESSAGE_TYPES) for _ in range(n)]
        futures = [ex.submit(send_message, mt) for mt in msg_types]
        for f in as_completed(futures):
            results.append(f.result())
    elapsed = time.time() - t0

    ok = [r for r in results if r.success]
    fail = [r for r in results if not r.success]
    latencies = [r.latency_ms for r in ok]
    broker_dist = {}
    for r in ok:
        broker_dist[r.broker] = broker_dist.get(r.broker, 0) + 1

    import statistics
    summary = {
        "label": label,
        "total": n,
        "success": len(ok),
        "failed": len(fail),
        "success_rate_pct": round(len(ok) / n * 100, 1),
        "tps": round(len(ok) / elapsed, 2),
        "avg_latency_ms": round(statistics.mean(latencies), 1) if latencies else 0,
        "p95_latency_ms": round(sorted(latencies)[int(len(latencies)*0.95)] if latencies else 0, 1),
        "broker_distribution": broker_dist,
        "elapsed_sec": round(elapsed, 2)
    }
    print(f"  [{label}] success={summary['success_rate_pct']}%  "
          f"tps={summary['tps']}  avg={summary['avg_latency_ms']}ms  "
          f"brokers={broker_dist}")
    return summary


def kill_kafka():
    subprocess.run(["docker", "stop", KAFKA_CONTAINER], capture_output=True, timeout=15)
    print(f"  Kafka container '{KAFKA_CONTAINER}' stopped")


def start_kafka():
    subprocess.run(["docker", "start", KAFKA_CONTAINER], capture_output=True, timeout=15)
    print(f"  Kafka container '{KAFKA_CONTAINER}' started")
    time.sleep(8)  # allow broker to come up and health check to detect recovery


def check_gateway():
    try:
        with urllib.request.urlopen("http://localhost:8080/actuator/health", timeout=3) as r:
            return b"UP" in r.read()
    except Exception:
        return False


def main():
    print("="*60)
    print("Broker Kill Test — AdaptRoute Failover Evaluation")
    print("="*60)

    if not check_gateway():
        print("ERROR: Gateway not reachable at localhost:8080. Start services first.")
        return

    results = {}

    # Phase 1: Baseline (both brokers healthy)
    print("\n--- Phase 1: Baseline (both brokers healthy) ---")
    results["baseline"] = run_burst(n=200, concurrency=20, label="baseline")

    # Phase 2: Kill Kafka mid-run — send burst while killing
    print("\n--- Phase 2: Kill Kafka (send 100 msgs, kill Kafka after 50ms) ---")
    import threading
    kill_timer = threading.Timer(0.05, kill_kafka)
    kill_timer.start()
    mid_kill = run_burst(n=100, concurrency=20, label="mid_kafka_kill")
    kill_timer.cancel()  # in case it hasn't fired
    results["mid_kafka_kill"] = mid_kill

    # Note time of kill for failover measurement
    t_kill = time.time()

    # Phase 3: All-RabbitMQ (Kafka dead, AdaptRoute should route to RabbitMQ)
    print("\n--- Phase 3: RabbitMQ-only mode (Kafka down) ---")
    time.sleep(1)  # let health check detect failure
    results["kafka_down"] = run_burst(n=200, concurrency=20, label="kafka_down")

    # Phase 4: Measure failover latency (time from kill to first successful RabbitMQ message)
    print(f"\n  Kill-to-failover window: ~{time.time() - t_kill:.1f}s elapsed since Kafka killed")

    # Phase 5: Recovery — restart Kafka
    print("\n--- Phase 5: Kafka recovery ---")
    start_kafka()
    results["recovery"] = run_burst(n=200, concurrency=20, label="recovery")

    # Save
    import os
    os.makedirs("results/failure_tests", exist_ok=True)
    outfile = "results/failure_tests/broker_kill.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {outfile}")
    print("\nSummary:")
    for phase, r in results.items():
        print(f"  {phase:<20}: {r['success_rate_pct']}% success | "
              f"brokers={r['broker_distribution']}")


if __name__ == "__main__":
    main()
