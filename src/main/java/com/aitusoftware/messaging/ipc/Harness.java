package com.aitusoftware.messaging.ipc;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class Harness {
    private static final int MAX_VALUE = 1_000_000;
    private final OffHeapByteBufferTransport clientPublisher;
    private final OffHeapByteBufferTransport clientSubscriber;
    private final ByteBuffer message;
    private final Histogram histogram = new Histogram(MAX_VALUE, 3);
    private final OffHeapByteBufferTransport serverPublisher;
    private final OffHeapByteBufferTransport serverSubscriber;

    public static void main(String[] args) throws IOException {
        new Harness(Paths.get("/dev/shm/ipc"), 256).runLoop();
    }

    public Harness(Path ipcFile, int messageSize) throws IOException {
        if (Files.exists(ipcFile)) {
            Files.delete(ipcFile);
        }
        message = ByteBuffer.allocateDirect(messageSize);
        for (int i = 0; i < messageSize; i++) {
            message.put(i, (byte) 7);
        }
        message.clear();

        clientPublisher = new OffHeapByteBufferTransport(ipcFile, 1 << 23);
        clientSubscriber = new OffHeapByteBufferTransport(ipcFile, 1 << 23);
        serverPublisher = new OffHeapByteBufferTransport(ipcFile, 1 << 23);
        serverSubscriber = new OffHeapByteBufferTransport(ipcFile, 1 << 23);
    }

    void echoLoop()
    {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                serverSubscriber.poll(this::echoMessage);
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
        finally {
            System.out.println("Echo loop complete");
        }
    }

    void echoMessage(ByteBuffer message) {
        serverPublisher.writeRecord(message);
    }

    void runLoop() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(this::echoLoop);
        for (int i = 0; i < 10_000; i++) {
            message.putLong(0, System.nanoTime());
            clientPublisher.writeRecord(message);

            while (clientSubscriber.poll(this::receiveMessage) == 0)
            {
                // spin
            }
        }

        executor.shutdownNow();
        histogram.outputPercentileDistribution(System.out, 1d);
    }

    void receiveMessage(ByteBuffer message) {
        long rttNanos = System.nanoTime() - message.getLong(message.position());
        histogram.recordValue(Math.min(MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(rttNanos)));
    }

}