package com.aitusoftware.messaging.benchmarks;

import com.aitusoftware.messaging.ipc.OffHeapByteBufferTransport;
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
public class VarHandleIpcBenchmark
{
    private static final int MESSAGE_SIZE = 256;
    private static final int BUFFER_SIZE = 1 << 21;

    private ByteBuffer message;
    private OffHeapByteBufferTransport clientPublisher;

    @Setup
    public void setup() throws IOException
    {
        AffinityUtil.set();
        int messageSize = MESSAGE_SIZE;
        Path ipcFileIn = Paths.get("/dev/shm/ipc-in");
        if (Files.exists(ipcFileIn))
        {
            Files.delete(ipcFileIn);
        }

        message = ByteBuffer.allocateDirect(messageSize);
        for (int i = 0; i < messageSize; i++)
        {
            message.put(i, (byte) 7);
        }
        message.clear();

        clientPublisher = new OffHeapByteBufferTransport(ipcFileIn, BUFFER_SIZE);
    }

    @Benchmark
    public long publish()
    {
        message.clear();
        return clientPublisher.writeRecord(message);
    }
}