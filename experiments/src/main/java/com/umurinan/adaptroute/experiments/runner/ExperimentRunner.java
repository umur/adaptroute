package com.umurinan.adaptroute.experiments.runner;

import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.experiments.workload.WorkloadGenerator;
import com.umurinan.adaptroute.experiments.workload.WorkloadGenerator.DispatchRecord;
import com.umurinan.adaptroute.experiments.workload.WorkloadProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.client.RestTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;

/**
 * Experiment orchestrator for the adaptive message routing research paper.
 *
 * <h2>Execution Plan</h2>
 *
 * <p>Runs 30 repetitions of each (workload, strategy) combination:
 * 4 workloads × 4 strategies × 30 reps = 480 total runs.</p>
 *
 * <p>Output: {@code results/experiment_results.csv} with columns matching
 * what {@code generate_figures.py} expects:
 * workload, strategy, run, latency_p50, latency_p95, latency_p99,
 * throughput, delivery_rate, routing_overhead_ms, cpu_percent, memory_mb</p>
 *
 * <p>Workload names in CSV: W1, W2, W3, W4.<br>
 * Strategy names in CSV: kafka_only, rabbitmq_only, static_split, adaptroute.</p>
 */
@Slf4j
@SpringBootApplication(scanBasePackages = "com.umurinan.adaptroute.experiments")
@RequiredArgsConstructor
public class ExperimentRunner implements CommandLineRunner {

    private final WorkloadGenerator generator;
    private final RestTemplate       restTemplate;

    @Value("${gateway.url:http://gateway-service:8080}")
    private String gatewayUrl;

    @Value("${results.dir:results}")
    private String resultsDir;

    @Value("${experiment.cooldown-ms:5000}")
    private long cooldownMs;

    @Value("${experiment.repetitions:30}")
    private int repetitions;

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault());

    // CSV column names expected by generate_figures.py
    private static final String[] CSV_HEADERS = {
            "workload", "strategy", "run",
            "latency_p50", "latency_p95", "latency_p99",
            "throughput", "delivery_rate", "routing_overhead_ms",
            "cpu_percent", "memory_mb"
    };

    // Workload label → CSV name
    private static final java.util.Map<String, String> WORKLOAD_CSV_NAME =
            java.util.Map.of(
                    "HIGH_THROUGHPUT_BURST", "W1",
                    "LOW_LATENCY_STREAM",    "W2",
                    "MIXED_WORKLOAD",        "W3",
                    "ORDERING_CRITICAL",     "W4"
            );

    // Strategy: BrokerTarget → CSV strategy name
    private static final java.util.Map<BrokerTarget, String> STRATEGY_CSV_NAME =
            java.util.Map.of(
                    BrokerTarget.KAFKA,        "kafka_only",
                    BrokerTarget.RABBITMQ,     "rabbitmq_only",
                    BrokerTarget.STATIC_SPLIT, "static_split",
                    BrokerTarget.ADAPTIVE,     "adaptroute"
            );

    public static void main(String[] args) {
        SpringApplication.run(ExperimentRunner.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String runTimestamp = TS_FMT.format(Instant.now());
        log.info("=================================================================");
        log.info("Adaptive Message Routing — Experiment Suite");
        log.info("Run timestamp  : {}", runTimestamp);
        log.info("Gateway URL    : {}", gatewayUrl);
        log.info("Repetitions    : {}", repetitions);
        log.info("Results dir    : {}", resultsDir);
        log.info("=================================================================");

        waitForGateway();

        Path outputDir = Paths.get(resultsDir);
        Files.createDirectories(outputDir);
        Path csvPath = outputDir.resolve("experiment_results.csv");

        List<WorkloadProfile> profiles = List.of(
                WorkloadProfile.highThroughputBurst(),
                WorkloadProfile.lowLatencyStream(),
                WorkloadProfile.mixedWorkload(),
                WorkloadProfile.orderingCritical()
        );

        List<BrokerTarget> strategies = List.of(
                BrokerTarget.KAFKA,
                BrokerTarget.RABBITMQ,
                BrokerTarget.STATIC_SPLIT,
                BrokerTarget.ADAPTIVE
        );

        List<ResultRow> allRows = new ArrayList<>();
        int totalConfigs = profiles.size() * strategies.size();
        int configsDone  = 0;

        for (WorkloadProfile profile : profiles) {
            String workloadLabel = WORKLOAD_CSV_NAME.getOrDefault(profile.getName(), profile.getName());
            log.info("\n=== Workload: {} ({}) ===", profile.getName(), workloadLabel);

            for (BrokerTarget strategy : strategies) {
                String strategyLabel = STRATEGY_CSV_NAME.get(strategy);
                configsDone++;
                log.info("  [{}/{}] strategy={} reps={}",
                        configsDone, totalConfigs, strategyLabel, repetitions);

                for (int rep = 1; rep <= repetitions; rep++) {
                    log.info("    rep {}/{} ...", rep, repetitions);

                    long wallStart = System.currentTimeMillis();
                    List<DispatchRecord> records = generator.run(profile, strategy, gatewayUrl);
                    long wallMs = System.currentTimeMillis() - wallStart;

                    ResultRow row = summariseRun(workloadLabel, strategyLabel, rep,
                            records, wallMs, strategy);
                    allRows.add(row);

                    log.info("    rep {}: p50={:.2f}ms p95={:.2f}ms p99={:.2f}ms "
                                    + "throughput={:.0f}msg/s delivery={:.4f}",
                            rep, row.latencyP50, row.latencyP95, row.latencyP99,
                            row.throughput, row.deliveryRate);

                    // Short cooldown between repetitions
                    if (rep < repetitions) {
                        Thread.sleep(cooldownMs);
                    }
                }

                // Longer cooldown between strategy/workload combos
                log.info("  Cooldown {}ms after strategy={}...", cooldownMs * 2, strategyLabel);
                Thread.sleep(cooldownMs * 2L);
            }
        }

        writeResultsCsv(allRows, csvPath);

        log.info("=================================================================");
        log.info("Experiment complete. {} rows written to: {}", allRows.size(), csvPath.toAbsolutePath());
        log.info("=================================================================");
    }

    // =========================================================================
    // Per-run summary computation
    // =========================================================================

    private ResultRow summariseRun(String workload, String strategy, int run,
                                    List<DispatchRecord> records, long wallMs,
                                    BrokerTarget brokerTarget) {

        long total   = records.size();
        long success = records.stream().filter(DispatchRecord::success).count();

        // Latency array (in MICROSECONDS from DispatchRecord), converted to ms for CSV
        long[] latMicros = records.stream()
                .filter(DispatchRecord::success)
                .mapToLong(DispatchRecord::latencyMicros)
                .sorted()
                .toArray();

        LongSummaryStatistics latStats = records.stream()
                .filter(DispatchRecord::success)
                .mapToLong(DispatchRecord::latencyMicros)
                .summaryStatistics();

        double p50Ms = latMicros.length > 0 ? percentile(latMicros, 50) / 1000.0 : 0.0;
        double p95Ms = latMicros.length > 0 ? percentile(latMicros, 95) / 1000.0 : 0.0;
        double p99Ms = latMicros.length > 0 ? percentile(latMicros, 99) / 1000.0 : 0.0;

        // Throughput = total messages / wall-clock seconds
        double wallSec     = Math.max(wallMs, 1) / 1000.0;
        double throughput  = total / wallSec;

        // Delivery rate = success / total
        double deliveryRate = total > 0 ? (double) success / total : 0.0;

        // Routing overhead: mean scoring decision latency reported by the gateway.
        // For adaptive routing, the gateway runs the weighted scoring model and returns
        // decisionLatencyMicros in the RoutingDecision response. Baselines bypass scoring
        // (routingHintApplied=true) so their decisionLatencyMicros is near zero.
        // We report 0.0 explicitly for baselines to make the distinction clear in the CSV.
        double routingOverheadMs;
        if (brokerTarget == BrokerTarget.ADAPTIVE) {
            routingOverheadMs = records.stream()
                    .filter(DispatchRecord::success)
                    .mapToLong(DispatchRecord::decisionLatencyMicros)
                    .average()
                    .orElse(0.0) / 1000.0;
        } else {
            routingOverheadMs = 0.0;
        }

        // cpu_percent and memory_mb: not directly measurable from JVM without JMX/proc.
        // Use 0.0 — generate_figures.py does not plot these directly but they appear in the CSV.
        double cpuPercent = 0.0;
        double memoryMb   = 0.0;

        return new ResultRow(workload, strategy, run,
                p50Ms, p95Ms, p99Ms,
                throughput, deliveryRate, routingOverheadMs,
                cpuPercent, memoryMb);
    }

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        int index = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.min(Math.max(index, 0), sorted.length - 1)];
    }

    // =========================================================================
    // CSV output
    // =========================================================================

    private void writeResultsCsv(List<ResultRow> rows, Path file) throws IOException {
        try (FileWriter fw = new FileWriter(file.toFile());
             CSVPrinter csv = new CSVPrinter(fw, CSVFormat.DEFAULT.builder()
                     .setHeader(CSV_HEADERS).build())) {

            for (ResultRow r : rows) {
                csv.printRecord(
                        r.workload,
                        r.strategy,
                        r.run,
                        String.format("%.6f", r.latencyP50),
                        String.format("%.6f", r.latencyP95),
                        String.format("%.6f", r.latencyP99),
                        String.format("%.4f", r.throughput),
                        String.format("%.6f", r.deliveryRate),
                        String.format("%.6f", r.routingOverheadMs),
                        String.format("%.4f", r.cpuPercent),
                        String.format("%.4f", r.memoryMb)
                );
            }
        }
        log.info("Wrote {} rows to {}", rows.size(), file.toAbsolutePath());
    }

    // =========================================================================
    // Gateway readiness probe
    // =========================================================================

    private void waitForGateway() throws InterruptedException {
        log.info("Waiting for gateway service at {}...", gatewayUrl);
        int attempts = 0;
        while (attempts < 30) {
            try {
                var response = restTemplate.getForEntity(gatewayUrl + "/api/status", String.class);
                if (response.getStatusCode().is2xxSuccessful()) {
                    log.info("Gateway is ready.");
                    return;
                }
            } catch (Exception ex) {
                log.debug("Gateway not ready yet (attempt {}): {}", attempts + 1, ex.getMessage());
            }
            attempts++;
            Thread.sleep(2_000);
        }
        throw new IllegalStateException("Gateway service did not become ready after 60 seconds");
    }

    // =========================================================================
    // Result row record
    // =========================================================================

    private record ResultRow(
            String workload,
            String strategy,
            int    run,
            double latencyP50,
            double latencyP95,
            double latencyP99,
            double throughput,
            double deliveryRate,
            double routingOverheadMs,
            double cpuPercent,
            double memoryMb
    ) {}
}
