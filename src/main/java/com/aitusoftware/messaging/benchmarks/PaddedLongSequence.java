package com.aitusoftware.messaging.benchmarks;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PaddedLongSequence {

    private static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    private static final VarHandle VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private final ByteBuffer data;
    private final int sequenceOffset = CACHE_LINE_SIZE_IN_BYTES;

    public PaddedLongSequence(ByteBuffer data) {
        this.data = data;

        if (data.remaining() < 2 * CACHE_LINE_SIZE_IN_BYTES) {
            throw new IllegalArgumentException();
        }
        if (data.alignmentOffset(0, 8) != 0) {
            throw new IllegalArgumentException();
        }
    }

    public boolean compareAndSwap(long expected, long update) {
        return ((long) VIEW.compareAndExchange(
                data, sequenceOffset, expected, update)) == expected;
    }




    public static void main(String[] args) {
        PaddedLongSequence sequence = new PaddedLongSequence(ByteBuffer.allocate(256));

        System.out.println(sequence.compareAndSwap(0, 1));
        System.out.println(sequence.compareAndSwap(1, 2));
        System.out.println(sequence.compareAndSwap(1, 2));
    }
}