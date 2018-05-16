package com.aitusoftware.messaging.ipc;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

public final class UnsafeHarness
{
    private static final int MESSAGE_COUNT = Integer.getInteger("ipc.msgCount", 1 << 20);
    private static final long MAX_VALUE = TimeUnit.MILLISECONDS.toNanos(5L);
    private static final int BUFFER_SIZE = Integer.getInteger("ipc.bufferSize", MESSAGE_COUNT / 8);
    private static final int MESSAGE_SIZE = Integer.getInteger("ipc.msgSize", 256);
    private final UnsafeBufferTransport clientPublisher;
    private final UnsafeBufferTransport clientSubscriber;
    private final UnsafeBufferTransport serverPublisher;
    private final UnsafeBufferTransport serverSubscriber;
    private final UnsafeBuffer message;
    private final Histogram histogram = new Histogram(MAX_VALUE, 3);
    private final int sequenceOffset;
    private long sequence;
    private long expectedSequence;
    private int count = 0;

    public static void main(String[] args) throws IOException
    {
        new UnsafeHarness(Paths.get("/dev/shm/ipc-in"),
                Paths.get("/dev/shm/ipc-out"), MESSAGE_SIZE).runLoop();
    }

    public UnsafeHarness(Path ipcFileIn, Path ipcFileOut, int messageSize) throws IOException
    {
        if (Files.exists(ipcFileIn))
        {
            Files.delete(ipcFileIn);
        }
        if (Files.exists(ipcFileOut))
        {
            Files.delete(ipcFileOut);
        }
        ByteBuffer message = ByteBuffer.allocateDirect(messageSize);
        for (int i = 0; i < messageSize; i++)
        {
            message.put(i, (byte) 7);
        }
        message.clear();
        this.message = new UnsafeBuffer(message);
        this.sequenceOffset = messageSize - 8;

        clientPublisher = new UnsafeBufferTransport(ipcFileIn, BUFFER_SIZE);
        clientSubscriber = new UnsafeBufferTransport(ipcFileOut, BUFFER_SIZE);
        serverPublisher = new UnsafeBufferTransport(ipcFileOut, BUFFER_SIZE);
        serverSubscriber = new UnsafeBufferTransport(ipcFileIn, BUFFER_SIZE);
    }

    private void echoLoop()
    {
        Thread.currentThread().setName("echo");
        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                serverSubscriber.poll(this::echoMessage);
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    private void echoMessage(UnsafeBuffer message)
    {
        serverPublisher.writeRecord(message);
    }

    private void runLoop()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.submit(this::echoLoop);
        Future<?> future = executor.submit(this::receiveLoop);
        Thread.currentThread().setName("harness");
        try
        {
            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                message.putLong(sequenceOffset, sequence++);
                message.putLong(0, System.nanoTime());
                try
                {
                    clientPublisher.writeRecord(message);
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    return;
                }

            }
            future.get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException | TimeoutException e)
        {
            e.printStackTrace();
        }
        finally
        {
            executor.shutdownNow();
            histogram.outputPercentileDistribution(System.out, 1d);
        }
    }

    private void receiveLoop()
    {
        while (count < MESSAGE_COUNT)
        {
            clientSubscriber.poll(this::receiveMessage);
        }
    }

    private void receiveMessage(UnsafeBuffer message)
    {
        long rttNanos = System.nanoTime() - message.getLong(0);
        count++;
        histogram.recordValue(Math.min(MAX_VALUE, rttNanos));
        if (message.getLong(sequenceOffset) != expectedSequence)
        {
            System.err.printf("Expected sequence %d, but was %d",
                    expectedSequence, sequenceOffset);
            throw new RuntimeException(String.format("Expected sequence %d, but was %d",
                    expectedSequence, sequenceOffset));
        }
        expectedSequence++;
    }
}