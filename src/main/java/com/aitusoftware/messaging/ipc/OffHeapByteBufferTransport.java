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

public final class OffHeapByteBufferTransport
{
    private static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    private static final int DATA_OFFSET = CACHE_LINE_SIZE_IN_BYTES * 4;
    private static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;
    private static final VarHandle VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private final ByteBuffer data;
    private final ByteBuffer messageBuffer;
    private static final int PUBLISHER_SEQUENCE_OFFSET = 8 * 7;
    private static final int SUBSCRIBER_SEQUENCE_OFFSET = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    private final long mask;
    private final FileChannel channel;
    private long writeOffset;

    public OffHeapByteBufferTransport(Path path, long size) throws IOException
    {
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
    }

    private long lastConsumedSequence = 0L;
    private long writeLimit = -1L;

    public long writeRecord(final ByteBuffer message)
    {
        if (writeLimit == -1L)
        {
            writeLimit = getSubscriberOffset() + messageBuffer.capacity();
        }
        if (writeOffset + message.remaining() > writeLimit)
        {
            writeLimit = getSubscriberOffset() + messageBuffer.capacity();
        }

        if (writeOffset + message.remaining() > writeLimit)
        {
            while (writeOffset + message.remaining() > writeLimit) {
                writeLimit = getSubscriberOffset() + messageBuffer.capacity();
            }
        }

        final int messageSize = message.remaining();
        if (messageSize == 0)
        {
            return -1;
        }
        final int paddedSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
        writeOffset = (long) VIEW.getAndAdd(data, PUBLISHER_SEQUENCE_OFFSET,
                paddedSize);
        final int actualOffset = mask(writeOffset) + MESSAGE_HEADER_LENGTH;
        messageBuffer.position(actualOffset);
        messageBuffer.put(message);
        VIEW.setRelease(messageBuffer, mask(writeOffset), (long) messageSize);
        return writeOffset;
    }


    public int poll(final Consumer<ByteBuffer> receiver)
    {
        final int messageSize = (int) ((long) VIEW.getVolatile(messageBuffer, mask(lastConsumedSequence)));
        if (messageSize != 0)
        {
            final int newPosition = mask(lastConsumedSequence + MESSAGE_HEADER_LENGTH);
            final int newLimit = newPosition + messageSize;
            messageBuffer.limit(newLimit);
            messageBuffer.position(newPosition);
            receiver.accept(messageBuffer);
            VIEW.setRelease(data, SUBSCRIBER_SEQUENCE_OFFSET, lastConsumedSequence);
            // program order should ensure the next read sees zero, unless written by the producer
            VIEW.set(data, newPosition - MESSAGE_HEADER_LENGTH, 0L);
            lastConsumedSequence += padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
            messageBuffer.limit(messageBuffer.capacity());
        }

        return messageSize;
    }

    private long getSubscriberOffset()
    {
        return (long) VIEW.getVolatile(data, SUBSCRIBER_SEQUENCE_OFFSET);
    }

    void sync() throws IOException
    {
        channel.force(true);
    }

    private static int padToCacheLine(int messageSize)
    {
        return messageSize + 64 - ((messageSize) & 63);
    }

    public static void main(String[] args) throws IOException
    {
        final Path ipcFile = Paths.get("/dev/shm/ipc");
        if (Files.exists(ipcFile))
        {
            Files.delete(ipcFile);
        }
        final OffHeapByteBufferTransport publishTransport =
                new OffHeapByteBufferTransport(ipcFile, 4096);
        final OffHeapByteBufferTransport subscribeTransport =
                new OffHeapByteBufferTransport(ipcFile, 4096);
        for (int i = 0; i < 400; i++)
        {
            if (i % 10 == 0)
            {
                while ((subscribeTransport.poll(subscribeTransport::printRecord)) != 0)
                {
                    // spin
                }
            }
            publishTransport.writeRecord(ByteBuffer.wrap(("some data " + i)
                    .getBytes(StandardCharsets.UTF_8)));
        }
    }

    void printRecord(ByteBuffer b)
    {
        byte[] tmp = new byte[b.remaining()];
        b.get(tmp);
        System.out.printf("%s%n", new String(tmp, StandardCharsets.UTF_8));
    }

    private int mask(long sequence)
    {
        return (int) (sequence & mask);
    }
}