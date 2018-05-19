package com.aitusoftware.messaging.benchmarks;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@Measurement(iterations = 10)
@Warmup(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
public class DataCopyBenchmark {
    private static final int MAX_PAYLOAD_SIZE = 2097152;
    private static final byte ZERO = (byte) 0;
    private final UnsafeBuffer agronaBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_PAYLOAD_SIZE));
    private final ByteBuffer nativeBuffer = ByteBuffer.allocateDirect(MAX_PAYLOAD_SIZE);
    private ByteBuffer source;

    @Param(value = {"64", "256", "1024", "4096", "262144", "2097152"})
    private int payloadSize;
    private int eightByteStepCount;

    @Setup
    public void setup() {
        AffinityUtil.set();
        source = ByteBuffer.allocateDirect(payloadSize);
        for (int i = 0; i < payloadSize / 8; i += 8) {
            source.putLong(i / 8, (byte) i);
        }
        eightByteStepCount = payloadSize / 8;
    }

    @Benchmark
    public long copyUnsafeBuffer() {
        source.clear();
        agronaBuffer.putBytes(0, source, payloadSize);
        return source.position();
    }

    @Benchmark
    @Fork(jvmArgsPrepend = "-Dagrona.disable.bounds.checks=true")
    public long copyUnsafeBufferWithoutBoundsCheck() {
        source.clear();
        agronaBuffer.putBytes(0, source, payloadSize);
        return source.position();
    }

    @Benchmark
    public long copyByteBuffer() {
        source.clear();
        nativeBuffer.clear();
        nativeBuffer.put(source);
        return source.position();
    }

    @Benchmark
    public long copyByteBufferEightBytes() {
        source.clear();
        nativeBuffer.clear();

        for (int i = 0; i < eightByteStepCount; i += 8) {
            nativeBuffer.putLong(i, source.get(i));
        }

        return nativeBuffer.get(0);
    }

    @Benchmark
    public long zeroUnsafeBuffer() {
        agronaBuffer.setMemory(0, payloadSize, ZERO);
        return agronaBuffer.getLong(0);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = "-Dagrona.disable.bounds.checks=true")
    public long zeroUnsafeBufferWithoutBoundsCheck() {
        agronaBuffer.setMemory(0, payloadSize, ZERO);
        return agronaBuffer.getLong(0);
    }

    @Benchmark
    public long zeroByteBuffer() {
        for (int i = 0; i < payloadSize; i++) {
            nativeBuffer.put(i, ZERO);
        }
        return nativeBuffer.get(0);
    }

    @Benchmark
    public long zeroByteBufferEightBytes() {
        for (int i = 0; i < eightByteStepCount; i += 8) {
            nativeBuffer.putLong(i, 0);
        }
        return nativeBuffer.get(0);
    }
}