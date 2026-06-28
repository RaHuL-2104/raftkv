package com.raftkv.benchmarks;

import com.raftkv.statemachine.KeyValueStateMachine;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures raw throughput of the state machine in isolation — no Raft,
 * no networking, no disk I/O. This is the absolute ceiling for how fast
 * the system could ever go if consensus and durability were free.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StateMachineBenchmark {

    private KeyValueStateMachine stateMachine;
    private AtomicInteger counter;

    @Setup(Level.Trial)
    public void setup() {
        stateMachine = new KeyValueStateMachine();
        counter = new AtomicInteger(0);
        // Pre-populate so GETs have something real to find
        for (int i = 0; i < 1000; i++) {
            stateMachine.apply("PUT key" + i + " value" + i);
        }
    }

    @Benchmark
    public String put() {
        int i = counter.incrementAndGet() % 1000;
        return stateMachine.apply("PUT key" + i + " value" + i);
    }

    @Benchmark
    public String get() {
        int i = counter.incrementAndGet() % 1000;
        return stateMachine.apply("GET key" + i);
    }

    @Benchmark
    public String delete() {
        int i = counter.incrementAndGet() % 1000;
        return stateMachine.apply("DELETE key" + i);
    }
}
