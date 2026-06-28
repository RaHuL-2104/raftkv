package com.raftkv.benchmarks;

import com.raftkv.config.NodeConfig;
import com.raftkv.raft.RaftNode;
import org.openjdk.jmh.annotations.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures the full submitCommand() -> append -> commit -> apply pipeline
 * on a single-node "cluster" (no peers, so it's its own majority and
 * commits immediately without waiting on network round trips). This
 * isolates the cost of OUR code — log append, WAL write, commit index
 * advancement, state machine application — separate from any network
 * latency a multi-node cluster would add on top.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class SingleNodeCommitBenchmark {

    private RaftNode node;
    private AtomicInteger counter;

    @Setup(Level.Trial)
    public void setup() throws InterruptedException {
        NodeConfig config = new NodeConfig(
                "bench-single-node", "localhost", 19999,
                List.of(), // no peers — single-node cluster
                150, 300, 50
        );
        node = new RaftNode(config);
        node.start();
        counter = new AtomicInteger(0);

        // Wait for it to elect itself leader (should be near-instant with no peers)
        long deadline = System.currentTimeMillis() + 2000;
        while (node.getRole() != com.raftkv.raft.RaftState.Role.LEADER
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        node.shutdown();
        try {
            java.nio.file.Path dir = java.nio.file.Path.of("data", "bench-single-node");
            if (java.nio.file.Files.exists(dir)) {
                java.nio.file.Files.walk(dir)
                        .sorted((a, b) -> b.compareTo(a))
                        .forEach(p -> {
                            try { java.nio.file.Files.deleteIfExists(p); } catch (Exception ignored) {}
                        });
            }
        } catch (Exception ignored) {}
    }

    @Benchmark
    public String submitAndCommit() throws Exception {
        int i = counter.incrementAndGet();
        return node.submitCommand("PUT benchKey" + i + " benchValue" + i)
                .get(2, TimeUnit.SECONDS);
    }
}
