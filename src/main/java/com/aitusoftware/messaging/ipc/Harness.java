package com.aitusoftware.messaging.ipc;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

public final class Harness {
    private static final int MESSAGE_COUNT = 1_000_000;
    private static final int MAX_VALUE = MESSAGE_COUNT;
//    private static final int BUFFER_SIZE = 1 << 23;
    private static final int BUFFER_SIZE = 2048;
    private final OffHeapByteBufferTransport clientPublisher;
    private final OffHeapByteBufferTransport clientSubscriber;
    private final ByteBuffer message;
    private final Histogram histogram = new Histogram(MAX_VALUE, 3);
    private final OffHeapByteBufferTransport serverPublisher;
    private final OffHeapByteBufferTransport serverSubscriber;

    public static void main(String[] args) throws IOException {
        new Harness(Paths.get("/dev/shm/ipc-in"),
                Paths.get("/dev/shm/ipc-out"), 256).runLoop();
    }

    public Harness(Path ipcFileIn, Path ipcFileOut, int messageSize) throws IOException {
        if (Files.exists(ipcFileIn)) {
            Files.delete(ipcFileIn);
        }
        if (Files.exists(ipcFileOut)) {
            Files.delete(ipcFileOut);
        }
        message = ByteBuffer.allocateDirect(messageSize);
        for (int i = 0; i < messageSize; i++) {
            message.put(i, (byte) 7);
        }
        message.clear();

        clientPublisher = new OffHeapByteBufferTransport(ipcFileIn, BUFFER_SIZE);
        clientSubscriber = new OffHeapByteBufferTransport(ipcFileOut, BUFFER_SIZE);
        serverPublisher = new OffHeapByteBufferTransport(ipcFileOut, BUFFER_SIZE);
        serverSubscriber = new OffHeapByteBufferTransport(ipcFileIn, BUFFER_SIZE);
    }

    void echoLoop()
    {
        Thread.currentThread().setName("echo");
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
//        System.out.println("echo " + count);
        serverPublisher.writeRecord(message);
    }

    void runLoop() {
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.submit(this::echoLoop);
        Future<?> future = executor.submit(this::receiveLoop);
        Thread.currentThread().setName("harness");
        try {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                message.clear();
                message.putLong(0, System.nanoTime());
                try {
                    clientPublisher.writeRecord(message);
                } catch (Throwable t) {
                    System.err.println(i);
                    t.printStackTrace();
                    return;
                }
            }
            future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
            histogram.outputPercentileDistribution(System.out, 1d);
        }
    }
    int count = 0;

    void receiveLoop() {
        while (count < MESSAGE_COUNT) {
            clientSubscriber.poll(this::receiveMessage);
        }
    }

    void receiveMessage(ByteBuffer message) {
        long rttNanos = System.nanoTime() - message.getLong(message.position());
//        if (++count % 10 == 0) {
//            System.out.println(TimeUnit.NANOSECONDS.toMillis(rttNanos));
//        }
        count++;
        histogram.recordValue(Math.min(MAX_VALUE, rttNanos));
    }

}