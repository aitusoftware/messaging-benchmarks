package com.aitusoftware.messaging;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

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

    @Setup
    public void setup() {
        source = ByteBuffer.allocateDirect(payloadSize);
        for (int i = 0; i < payloadSize / 8; i += 8) {
            source.putLong(i / 8, (byte) i);
        }
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
    public long copyByteBufferChunk() {
        source.clear();
        nativeBuffer.clear();
        return copyBufferChunks(source, nativeBuffer);
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
    public long zeroByteBufferFourBytes() {
        return zeroBuffer(0);
    }

    private long copyBufferChunks(ByteBuffer src, ByteBuffer dst) {
        int chunks = src.remaining() / 8;
        for (int i = 0; i < chunks; i++) {
            dst.putLong(src.getLong());
        }
        dst.put(src);
        return src.remaining();
    }

    private long zeroBuffer(int value) {
        long intValue = value;
        intValue |= value << 8;
        intValue |= value << 16;
        intValue |= value << 24;
        for (int i = 0; i < payloadSize / 4; i += 4) {
            nativeBuffer.putInt(i, (int) intValue);
        }
        return nativeBuffer.get(0);
    }
}