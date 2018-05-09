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
    private static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;
    private static final VarHandle VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private final ByteBuffer data;
    private final int publisherSequenceOffset = 8 * 7;
    private final int subscriberSequenceOffset = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    private final int dataOffset = CACHE_LINE_SIZE_IN_BYTES * 4;
    private final long mask;
    private final FileChannel channel;

    public OffHeapByteBufferTransport(Path path, long size) throws IOException
    {
        channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        final MappedByteBuffer data = channel.
                map(FileChannel.MapMode.READ_WRITE, 0L, size + HEADER_LENGTH + 8);


        if (Long.bitCount(size) != 1)
        {
            throw new IllegalArgumentException();
        }

        this.data = data.alignedSlice(8);
        this.mask = size - 1;

        if (data.remaining() < 2 * CACHE_LINE_SIZE_IN_BYTES)
        {
            throw new IllegalArgumentException();
        }
        if (data.alignmentOffset(0, 8) != 0)
        {
            throw new IllegalArgumentException();
        }
    }

    public long writeRecord(final ByteBuffer message)
    {
        final int messageSize = message.remaining();
        if (messageSize == 0)
        {
            return -1;
        }
        final int paddedSize = padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
        final long writeOffset = (long) VIEW.getAndAdd(data, publisherSequenceOffset,
                paddedSize) + HEADER_LENGTH;
        final int actualOffset = mask(writeOffset) + MESSAGE_HEADER_LENGTH;
        System.out.printf("Writing %d (%d) at %d (%d) (offset %d, value %d)%n", messageSize, paddedSize,
                writeOffset, actualOffset, publisherSequenceOffset,
                (long) VIEW.getVolatile(data, publisherSequenceOffset));
        data.position(actualOffset);
        data.put(message);
        VIEW.setRelease(data, (int) writeOffset, (long) messageSize);
        return writeOffset;
    }

    private long lastConsumedSequence = 0L;

    public int poll(final Consumer<ByteBuffer> receiver)
    {
        final int messageSize = (int) ((long) VIEW.getVolatile(data, mask(lastConsumedSequence) + HEADER_LENGTH));
        if (messageSize != 0)
        {
            final int newPosition = (int) (lastConsumedSequence + HEADER_LENGTH + MESSAGE_HEADER_LENGTH);
            final int newLimit = newPosition + messageSize;
            data.limit(newLimit);
            data.position(newPosition);
            receiver.accept(data);
            VIEW.setRelease(data, subscriberSequenceOffset, lastConsumedSequence);
            lastConsumedSequence += padToCacheLine(messageSize + MESSAGE_HEADER_LENGTH);
            data.limit(data.capacity());
        }
        return messageSize;
    }

    private long getSubscriberOffset()
    {
        return (long) VIEW.getVolatile(data, subscriberSequenceOffset);
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
        final OffHeapByteBufferTransport transport =
                new OffHeapByteBufferTransport(ipcFile, 4096);
        for (int i = 0; i < 10; i++)
        {
            transport.writeRecord(ByteBuffer.wrap(("some data " + i)
                    .getBytes(StandardCharsets.UTF_8)));
        }
        while ((transport.poll(transport::printRecord)) != 0)
        {
            // spin
        }
        transport.sync();
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

    /*
    |                    pub|                      |
    |                    sub|                      |
    | data ...
     */


    static final int HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES * 4;
}