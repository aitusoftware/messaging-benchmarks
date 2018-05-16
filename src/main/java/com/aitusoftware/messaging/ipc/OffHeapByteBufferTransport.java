package com.aitusoftware.messaging.ipc;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public final class OffHeapByteBufferTransport implements AutoCloseable
{
    private static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    private static final int DATA_OFFSET = CACHE_LINE_SIZE_IN_BYTES * 4;
    private static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;
    private static final int PUBLISHER_SEQUENCE_OFFSET = 8 * 7;
    private static final int SUBSCRIBER_SEQUENCE_OFFSET = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    private static final boolean DEBUG = false;
    private static final VarHandle VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private final ByteBuffer data;
    private final ByteBuffer messageBuffer;
    private final long mask;
    private final FileChannel channel;
    private final Path path;

    // publisher state
    private long writeOffset;
    private long nextSubscriberSequenceCheck = -1L;
    private long nextBufferWrapSequence;

    // subscriber state
    private long lastConsumedSequence = 0L;


    public OffHeapByteBufferTransport(Path path, long size) throws IOException
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

        this.data = data.alignedSlice(8);
        this.messageBuffer = data.position(DATA_OFFSET).limit(DATA_OFFSET + (int) size).slice();
        this.data.clear();
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


    public long writeRecord(final ByteBuffer message)
    {
        if (nextSubscriberSequenceCheck == -1L)
        {
            nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
        }
        if (writeOffset + message.remaining() > nextSubscriberSequenceCheck)
        {
            nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
        }

        if (writeOffset + message.remaining() > nextSubscriberSequenceCheck)
        {
            if (DEBUG) {
                System.out.printf("%s %s writeOffset: %d, subscriber: %d%n",
                        path, Thread.currentThread().getName(), writeOffset, getSubscriberOffset());
            }
            while (writeOffset + message.remaining() > nextSubscriberSequenceCheck) {
                nextSubscriberSequenceCheck = getSubscriberOffset() + messageBuffer.capacity();
            }
        }

        final int messageSize = message.remaining();

        if (messageSize == 0)
        {
            return -1;
        }
        int paddedSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);

        writeOffset = (long) VIEW.getAndAdd(data, PUBLISHER_SEQUENCE_OFFSET, paddedSize);

        if (writeOffset + paddedSize > nextBufferWrapSequence) {
            if (DEBUG) {
                System.out.printf("%s %s Buffer overrun, writing %d at %d and attempting another message%n",
                        path, Thread.currentThread().getName(), -paddedSize, mask(writeOffset));
            }

            nextBufferWrapSequence = nextBufferWrapSequence + messageBuffer.capacity();

            final int forwardingPointerPosition = mask(writeOffset);
            long retryResult = writeRecord(message);
            VIEW.setRelease(messageBuffer, forwardingPointerPosition, (long) -paddedSize);
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
            messageBuffer.position(actualOffset);
            messageBuffer.put(message);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
        VIEW.setRelease(messageBuffer, headerOffset, (long) messageSize);
        return writeOffset;
    }


    public int poll(final Consumer<ByteBuffer> receiver)
    {
        int messageSize = (int) ((long) VIEW.getVolatile(messageBuffer, mask(lastConsumedSequence)));
        if (messageSize < 0L) {

            this.lastConsumedSequence -= messageSize;
            messageSize = (int) ((long) VIEW.getVolatile(messageBuffer, mask(this.lastConsumedSequence)));
        }
        if (messageSize != 0)
        {
            final int newPosition = mask(lastConsumedSequence + MESSAGE_HEADER_LENGTH);
            final int newLimit = newPosition + messageSize;
            int headerOffset = newPosition - MESSAGE_HEADER_LENGTH;
            if (DEBUG) {
                System.out.printf("%s %s Read message of %db at %d [%d]%n",
                        path, Thread.currentThread().getName(),
                        messageSize, headerOffset, lastConsumedSequence);
            }
            try {
                messageBuffer.limit(newLimit);
            } catch (RuntimeException e) {
                System.out.printf("newLimit: %d, newPosition: %d, from messageSize: %d at %d%n",
                        newLimit, newPosition, messageSize, lastConsumedSequence);
                throw e;
            }
            messageBuffer.position(newPosition);
            receiver.accept(messageBuffer);
            messageBuffer.position(headerOffset);
            int paddedMessageSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
            messageBuffer.limit(headerOffset + paddedMessageSize);
            zero(messageBuffer);

            VIEW.setRelease(data, SUBSCRIBER_SEQUENCE_OFFSET, lastConsumedSequence);
            lastConsumedSequence += paddedMessageSize;
            messageBuffer.limit(messageBuffer.capacity());
            if (DEBUG) {
                System.out.printf("%s %s read sequence advanced to %d%n", path,
                        Thread.currentThread().getName(), lastConsumedSequence);
            }
        }

        return messageSize;
    }

    public void close() throws IOException {
        channel.close();
    }

    private void zero(ByteBuffer buffer) {
        if (DEBUG) {
            System.out.printf("%s %s Zeroing buffer at %d - %d%n",
                    path, Thread.currentThread().getName(), buffer.position(), buffer.limit());
        }
        int chunks = buffer.remaining() / 8;
        for (int i = 0; i < chunks; i++) {
            buffer.putLong(0);
        }
        for (int i = 0; i < buffer.remaining(); i++) {
            buffer.put((byte) 0);
        }
    }

    private long getSubscriberOffset()
    {
        return (long) VIEW.getVolatile(data, SUBSCRIBER_SEQUENCE_OFFSET);
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