package com.aitusoftware.messaging.benchmarks;

import com.aitusoftware.messaging.ipc.OffHeapByteBufferTransport;
import com.aitusoftware.messaging.ipc.UnsafeBufferTransport;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@Measurement(iterations = 10)
@Warmup(iterations = 10)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Fork(value = 2)
@State(Scope.Benchmark)
public class UnsafeIpcBenchmark
{
    private static final int MESSAGE_SIZE = 256;
    private static final int BUFFER_SIZE = 1 << 21;

    private UnsafeBuffer message;
    private UnsafeBufferTransport clientPublisher;

    @Setup
    public void setup() throws IOException
    {
        AffinityUtil.set();
        System.setProperty(OffHeapByteBufferTransport.IPC_DISABLE_SUBSCRIBER_GATE, "true");
        int messageSize = MESSAGE_SIZE;
        Path ipcFileIn = Paths.get("/dev/shm/ipc-in");
        if (Files.exists(ipcFileIn))
        {
            Files.delete(ipcFileIn);
        }

        message = new UnsafeBuffer(ByteBuffer.allocateDirect(messageSize));
        for (int i = 0; i < messageSize; i++)
        {
            message.putByte(i, (byte) 7);
        }

        clientPublisher = new UnsafeBufferTransport(ipcFileIn, BUFFER_SIZE);
    }

    @Benchmark
    public long publish()
    {
        return clientPublisher.writeRecord(message);
    }
}