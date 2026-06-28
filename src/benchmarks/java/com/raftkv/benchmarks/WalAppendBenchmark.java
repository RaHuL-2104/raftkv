package com.raftkv.benchmarks;

import com.raftkv.storage.WalRecord;
import com.raftkv.storage.WriteAheadLog;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the real cost of a single durable WAL append, including the
 * fsync. This is usually the true bottleneck in any durable system —
 * worth measuring honestly rather than assuming it's negligible.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class WalAppendBenchmark {

    private static final String BENCH_NODE_ID = "benchmark-wal-node";

    private WriteAheadLog wal;
    private AtomicLong indexCounter;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        wal = new WriteAheadLog(BENCH_NODE_ID);
        wal.open();
        indexCounter = new AtomicLong(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        wal.close();
        Path dir = Path.of("data", BENCH_NODE_ID);
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
    }

    @Benchmark
    public void appendEntry() throws IOException {
        long index = indexCounter.incrementAndGet();
        wal.append(WalRecord.forEntry(1, index, "PUT benchKey benchValue"));
    }
}
