# AdaptRoute

A policy-driven middleware framework for runtime message broker selection in Spring Boot microservices.

AdaptRoute sits between Spring Boot producers and two message brokers (Apache Kafka and RabbitMQ), evaluating an asymmetric weighted scoring model at every dispatch to route each message class to the higher-scoring broker.

## Architecture

```
Producers --> Message Dispatcher --> Apache Kafka --> Consumers
                    |           --> RabbitMQ    --> Consumers
                    v
              Scoring Engine
                    ^
            Broker Load Monitor
```

## Modules

| Module | Description |
|--------|-------------|
| `common` | Shared DTOs, scoring engine (`WeightedScoringRouter`), broker load monitor |
| `gateway-service` | Message dispatcher, circuit breakers, failover buffer, health checks |
| `order-service` | ORDER message producer |
| `notification-service` | NOTIFICATION message producer |
| `analytics-service` | ANALYTICS message producer |
| `experiments` | Experiment runner and workload generator |
| `plots` | Python scripts for generating figures and statistical tests |

## Prerequisites

- Java 21 (OpenJDK)
- Docker & Docker Compose
- Maven 3.9+
- Python 3.10+ (for plots: `pip install -r plots/requirements.txt`)

## Quick Start

```bash
# Start the testbed (Kafka, RabbitMQ, all services)
docker compose up -d

# Run the full 480-run experiment suite
# (4 strategies x 4 workloads x 30 repetitions)
docker compose run experiments

# Generate figures
cd plots && python generate_figures.py
```

## Configuration

All routing parameters are Spring configuration properties:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `router.weights.kafka.*` | See paper Table 3 | Kafka scoring weights (sum = 1.0) |
| `router.weights.rabbit.*` | See paper Table 3 | RabbitMQ scoring weights (sum = 1.0) |
| `adaptroute.buffer.capacity` | 10,000 | Failover buffer size (messages) |
| `health.check.interval.ms` | 500 | Health probe interval |
| `health.check.timeout.ms` | 3,000 | AdminClient timeout |
| `load.ceiling.msgs` | 10,000 | Throughput ceiling for load factor |

## Experiment Workloads

| Workload | Rate | Composition | Purpose |
|----------|------|-------------|---------|
| W1 High-Throughput | 500 msg/s | 40% ORDER, 50% ANALYTICS, 10% NOTIFICATION | Flash-sale simulation |
| W2 Low-Latency | 300 msg/s | 90% NOTIFICATION (70% high-priority) | Real-time alerting |
| W3 Mixed Traffic | 400 msg/s | Equal thirds ORDER/NOTIFICATION/ANALYTICS | Balanced routing |
| W4 Ordering-Critical | 200 msg/s | 100% ORDER (EXACTLY_ONCE) | Ordering correctness |

## Reproducibility

- **Seeds**: `WORKLOAD_SEED=42`, `JITTER_SEED=137`
- **Docker images**: SHA256 digest-pinned in `docker-compose.yml`
- **Results**: Raw CSVs in `results/experiment_results.csv`
- **Figures**: Regenerate with `python plots/generate_figures.py`

## Paper

**AdaptRoute: A Policy-Driven Middleware Framework for Multi-Protocol Message Broker Selection in Spring Boot Microservices**

Umur Inan, Ata Turhan

Submitted to the Journal of Universal Computer Science (JUCS).

## License

MIT
