package com.aitusoftware.messaging.ipc;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

import static com.aitusoftware.messaging.ipc.Util.*;

public final class UnsafeBufferTransport implements AutoCloseable
{
    private static final boolean DEBUG = false;

    private final UnsafeBuffer data;
    private final UnsafeBuffer messageBuffer;
    private final long mask;
    private final FileChannel channel;
    private final Path path;

    // publisher state
    private long writeOffset;
    private long nextSubscriberSequenceCheck = -1L;
    private long nextBufferWrapSequence;

    // subscriber state
    private long lastConsumedSequence = 0L;
    private final UnsafeBuffer receiverView = new UnsafeBuffer();

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
        nextBufferWrapSequence = messageBuffer.capacity();
    }

    public long writeRecord(final UnsafeBuffer message)
    {
        final int messageSize = message.capacity();
        if (messageSize == 0) {
            return -1;
        }

        updateSubscriberCheckOffset(messageSize);
        waitForSlowSubscribers(messageSize);


        final int paddedSize = Util.padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);

        writeOffset = data.getAndAddLong(PUBLISHER_SEQUENCE_OFFSET, paddedSize);
        if (writeOffset + paddedSize > nextBufferWrapSequence) {
            if (DEBUG) {
                System.out.printf("%s %s Buffer overrun, writing %d at %d and attempting another message%n",
                        path, Thread.currentThread().getName(), -paddedSize, mask(writeOffset));
            }
            nextBufferWrapSequence = nextBufferWrapSequence + messageBuffer.capacity();

            final int forwardingPointPosition = mask(writeOffset);
            long retryResult = writeRecord(message);
            messageBuffer.putLongOrdered(forwardingPointPosition, (long) -paddedSize);
            return retryResult;
        }

        int headerOffset = mask(writeOffset);
        int actualOffset = headerOffset + MESSAGE_HEADER_LENGTH;
        try {
            if (DEBUG) {
                System.out.printf("%s %s Writing message of %db at %d [%d]%n",
                        path, Thread.currentThread().getName(),
                        paddedSize, headerOffset, writeOffset);
            }
            messageBuffer.putBytes(actualOffset, message, 0, messageSize);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
        messageBuffer.putLongOrdered(headerOffset, (long) messageSize);
        return writeOffset;
    }

    public int poll(final Consumer<UnsafeBuffer> receiver)
    {
        int messageSize = (int) messageBuffer.getLongVolatile(mask(lastConsumedSequence));

        if (messageSize < 0L) {
            this.lastConsumedSequence += -messageSize;
            messageSize = (int) ((long) messageBuffer.getLongVolatile(mask(this.lastConsumedSequence)));
        }
        if (messageSize != 0)
        {
            final int newPosition = mask(lastConsumedSequence + MESSAGE_HEADER_LENGTH);
            final int headerOffset = newPosition - MESSAGE_HEADER_LENGTH;
            if (DEBUG) {
                System.out.printf("%s %s Read message of %db at %d [%d]%n",
                        path, Thread.currentThread().getName(),
                        messageSize, newPosition - MESSAGE_HEADER_LENGTH, lastConsumedSequence);
            }
            receiverView.wrap(messageBuffer, newPosition, messageSize);
            receiver.accept(receiverView);
            final int paddedMessageSize = Util.padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
            receiverView.wrap(messageBuffer, headerOffset, paddedMessageSize);
            zero(receiverView);

            data.putLongOrdered(SUBSCRIBER_SEQUENCE_OFFSET, lastConsumedSequence);
            lastConsumedSequence += paddedMessageSize;
            if (DEBUG) {
                System.out.printf("%s %s read sequence advanced to %d%n", path,
                        Thread.currentThread().getName(), lastConsumedSequence);
            }
        }

        return messageSize;
    }

    private void waitForSlowSubscribers(int messageSize) {
        if (writeOffset + messageSize > nextSubscriberSequenceCheck)
        {
            if (DEBUG) {
                System.out.printf("%s %s writeOffset: %d, subscriber: %d%n",
                        path, Thread.currentThread().getName(), writeOffset, getSubscriberOffset());
            }
            while (writeOffset + messageSize > nextSubscriberSequenceCheck) {
                nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
            }
        }
    }

    private void updateSubscriberCheckOffset(int messageSize) {
        if (nextSubscriberSequenceCheck == -1L)
        {
            nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
        }
        if (writeOffset + messageSize > nextSubscriberSequenceCheck)
        {
            nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
        }
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
        return data.getLongVolatile(SUBSCRIBER_SEQUENCE_OFFSET);
    }

    private int mask(long sequence)
    {
        return (int) (sequence & mask);
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }
}