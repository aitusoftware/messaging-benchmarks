package com.aitusoftware.messaging.ipc;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public final class UnsafeBufferTransport
{
    private static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    private static final int DATA_OFFSET = CACHE_LINE_SIZE_IN_BYTES * 4;
    private static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;

    private final UnsafeBuffer data;
    private final UnsafeBuffer messageBuffer;
    private static final int PUBLISHER_SEQUENCE_OFFSET = 8 * 7;
    private static final int SUBSCRIBER_SEQUENCE_OFFSET = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    private static final boolean DEBUG = true;
    private final long mask;
    private final FileChannel channel;
    private final Path path;
    private long writeOffset;

    public UnsafeBufferTransport(Path path, long size) throws IOException
    {
        this.path = path;
        channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        final MappedByteBuffer data = channel.
                map(FileChannel.MapMode.READ_WRITE, 0L, size + DATA_OFFSET + 8);


        if (Long.bitCount(size) != 1)
        {
            throw new IllegalArgumentException();
        }

        ByteBuffer aligned = data.alignedSlice(8);
        this.data = new UnsafeBuffer(aligned);
        this.messageBuffer = new UnsafeBuffer(aligned, DATA_OFFSET, (int) size);
        this.mask = messageBuffer.capacity() - 1;

        if (data.remaining() < 2 * CACHE_LINE_SIZE_IN_BYTES)
        {
            throw new IllegalArgumentException();
        }
        if (data.alignmentOffset(0, 8) != 0)
        {
            throw new IllegalArgumentException();
        }
        nextOverrunCheck = messageBuffer.capacity();
    }

    private long lastConsumedSequence = 0L;
    private long writeLimit = -1L;
    private long nextOverrunCheck;

    public long writeRecord(final UnsafeBuffer message)
    {
        if (writeLimit == -1L)
        {
            writeLimit = getSubscriberOffset() + messageBuffer.capacity();
        }
        if (writeOffset + message.capacity() > writeLimit)
        {
            writeLimit = getSubscriberOffset() + messageBuffer.capacity();
        }

        if (writeOffset + message.capacity() > writeLimit)
        {
            if (DEBUG) {
                System.out.printf("%s %s writeOffset: %d, subscriber: %d%n",
                        path, Thread.currentThread().getName(), writeOffset, getSubscriberOffset());
            }
            while (writeOffset + message.capacity() > writeLimit) {
                writeLimit = getSubscriberOffset() + messageBuffer.capacity();
            }
        }

        final int messageSize = message.capacity();
        if (messageSize == 0)
        {
            return -1;
        }
        int paddedSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
        /*
        if writeOffset + paddedSize overruns buffer, claim remaining + paddedSize,
        write -1L to header, write new entry at 0
         */
        boolean overflow = false;


        writeOffset = (long) data.getAndAddLong(PUBLISHER_SEQUENCE_OFFSET,
                paddedSize);
        if (writeOffset + paddedSize > nextOverrunCheck) {
            if (DEBUG) {
                System.out.printf("%s %s Buffer overrun, writing %d at %d and attempting another message%n",
                        path, Thread.currentThread().getName(), -paddedSize, mask(writeOffset));
            }
//            paddedSize = (int) (nextOverrunCheck - writeOffset) + paddedSize;
            nextOverrunCheck = nextOverrunCheck + messageBuffer.capacity();

            int pointerPosition = mask(writeOffset);
            long retryResult = writeRecord(message);
            data.putLongOrdered(pointerPosition, (long) -paddedSize);
            long check = data.getLongVolatile(pointerPosition);
            if (check != -paddedSize) {
                System.out.printf("Check failed! %d != %d%n", -paddedSize, check);
            }
            return retryResult;
        }

        int actualOffset = mask(writeOffset) + MESSAGE_HEADER_LENGTH;
        int headerOffset = mask(writeOffset);
        try {
            if (DEBUG) {
                System.out.printf("%s %s Writing message of %db at %d [%d]%n",
                        path, Thread.currentThread().getName(),
                        paddedSize, headerOffset, writeOffset);
            }
            messageBuffer.putBytes(actualOffset, message, 0, messageSize);
        } catch (RuntimeException e) {
            System.out.printf("actualOffset: %d, limit: %d, capacity: %d, overflow: %s, writeOffset: %d, paddedSize: %d%n",
                    actualOffset, messageBuffer.capacity(), messageBuffer.capacity(),
                    overflow, writeOffset, paddedSize);
            throw e;
        }
        messageBuffer.putLongOrdered(headerOffset, (long) messageSize);
        long check = messageBuffer.getLongVolatile(headerOffset);
        if (check != messageSize) {
            System.out.printf("Check failed! %d != %d%n", messageSize, check);
        }
        return writeOffset;
    }

    private final UnsafeBuffer receiverView = new UnsafeBuffer();

    public int poll(final Consumer<UnsafeBuffer> receiver)
    {
        int messageSize = (int) ((long) data.getLongVolatile(mask(lastConsumedSequence)));
        boolean skipped = false;
        if (messageSize < 0L) {

            long previous = this.lastConsumedSequence;
            this.lastConsumedSequence += -messageSize;
            messageSize = (int) ((long) data.getLongVolatile(mask(this.lastConsumedSequence)));
            if (DEBUG) {
                System.out.printf("Read negative message size, moved seq from %d to %d, messageSize: %d%n",
                        previous, lastConsumedSequence, messageSize);
            }
            skipped = true;
        }
        if (messageSize != 0)
        {
            final int newPosition = mask(lastConsumedSequence + MESSAGE_HEADER_LENGTH);
            final int newLimit = newPosition + messageSize;
            if (DEBUG) {
                System.out.printf("%s %s Read message of %db at %d [%d]%n",
                        path, Thread.currentThread().getName(),
                        messageSize, newPosition - MESSAGE_HEADER_LENGTH, lastConsumedSequence);
            }
            receiverView.wrap(messageBuffer, newPosition, messageSize);
            receiver.accept(receiverView);
            receiverView.wrap(messageBuffer, newPosition - MESSAGE_HEADER_LENGTH,
                    padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH));
            zero(receiverView);

            data.putLongOrdered(SUBSCRIBER_SEQUENCE_OFFSET, lastConsumedSequence);
            // program order should ensure the next read sees zero, unless written by the producer
//            VIEW.set(data, newPosition - MESSAGE_HEADER_LENGTH, 0L);
            lastConsumedSequence += padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
            if (DEBUG) {
                System.out.printf("%s %s read sequence advanced to %d%n", path,
                        Thread.currentThread().getName(), lastConsumedSequence);
            }
        }

        return messageSize;
    }

    private void zero(UnsafeBuffer buffer) {

        int chunks = buffer.capacity() / 8;
        int bytes = buffer.capacity() - chunks * 8;
        for (int i = 0; i < chunks; i++) {
            buffer.putLong(i * 8, 0);
        }
        for (int i = 0; i < bytes; i++) {
            buffer.putLong(chunks * 8 + i, (byte) 0);
        }
        if (DEBUG) {
            System.out.printf("%s %s zeroed %db%n", path,
                    Thread.currentThread().getName(), chunks * 8 + bytes);
        }
    }

    private long getSubscriberOffset()
    {
        return (long) data.getLongVolatile(SUBSCRIBER_SEQUENCE_OFFSET);
    }

    void sync() throws IOException
    {
        channel.force(true);
    }

    private static int padToCacheLine(int messageSize)
    {
        return messageSize + 64 - ((messageSize) & 63);
    }

    private int mask(long sequence)
    {
        return (int) (sequence & mask);
    }
}