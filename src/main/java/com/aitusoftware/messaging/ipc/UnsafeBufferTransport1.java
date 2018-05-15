package com.aitusoftware.messaging.ipc;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public final class UnsafeBufferTransport1
{
    private static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    private static final int DATA_OFFSET = CACHE_LINE_SIZE_IN_BYTES * 4;
    private static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;

    private final UnsafeBuffer data;
    private final UnsafeBuffer messageBuffer;
    private static final int PUBLISHER_SEQUENCE_OFFSET = 8 * 7;
    private static final int SUBSCRIBER_SEQUENCE_OFFSET = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    private final long mask;
    private final FileChannel channel;
    private long writeOffset;

    public UnsafeBufferTransport1(Path path, long size) throws IOException
    {
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
    }

    private long lastConsumedSequence = 0L;
    private long writeLimit = -1L;

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
            while (writeOffset + message.capacity() > writeLimit) {
                writeLimit = getSubscriberOffset() + messageBuffer.capacity();
            }
        }

        final int messageSize = message.capacity();
        if (messageSize == 0)
        {
            return -1;
        }
        final int paddedSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
        writeOffset = data.getAndAddLong(PUBLISHER_SEQUENCE_OFFSET, paddedSize);
        final int actualOffset = mask(writeOffset) + MESSAGE_HEADER_LENGTH;
        messageBuffer.putBytes(actualOffset, message, 0, messageSize);
        data.putLongOrdered(mask(writeOffset), messageSize);
        return writeOffset;
    }

    private final UnsafeBuffer receiverView = new UnsafeBuffer();

    public int poll(final Consumer<UnsafeBuffer> receiver)
    {
        final int messageSize = (int) ((long) data.getLongVolatile(mask(lastConsumedSequence)));
        if (messageSize != 0)
        {
            final int newPosition = mask(lastConsumedSequence + MESSAGE_HEADER_LENGTH);
            final int newLimit = newPosition + messageSize;
            receiverView.wrap(messageBuffer, newPosition, messageSize);
            receiver.accept(receiverView);
            data.putLongOrdered(SUBSCRIBER_SEQUENCE_OFFSET, lastConsumedSequence);
            // program order should ensure the next read sees zero, unless written by the producer
            data.putLong(newPosition - MESSAGE_HEADER_LENGTH, 0L);
            lastConsumedSequence += padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
        }

        return messageSize;
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

    public static void main(String[] args) throws IOException
    {
        final Path ipcFile = Paths.get("/dev/shm/ipc");
        if (Files.exists(ipcFile))
        {
            Files.delete(ipcFile);
        }
        final UnsafeBufferTransport1 publishTransport =
                new UnsafeBufferTransport1(ipcFile, 4096);
        final UnsafeBufferTransport1 subscribeTransport =
                new UnsafeBufferTransport1(ipcFile, 4096);
        for (int i = 0; i < 400; i++)
        {
            if (i % 10 == 0)
            {
                while ((subscribeTransport.poll(subscribeTransport::printRecord)) != 0)
                {
                    // spin
                }
            }
            publishTransport.writeRecord(new UnsafeBuffer(("some data " + i)
                    .getBytes(StandardCharsets.UTF_8)));
        }
    }

    void printRecord(UnsafeBuffer b)
    {
        byte[] tmp = new byte[b.capacity()];
        b.getBytes(0, tmp);
        System.out.printf("%s%n", new String(tmp, StandardCharsets.UTF_8));
    }

    private int mask(long sequence)
    {
        return (int) (sequence & mask);
    }
}