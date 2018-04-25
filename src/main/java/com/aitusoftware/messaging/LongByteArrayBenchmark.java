/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.aitusoftware.messaging;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Measurement(iterations = 10)
@Warmup(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
public class LongByteArrayBenchmark
{
    private static final VarHandle LONG_ARRAY_VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final int BUFFER_OFFSET = 128;

    private final ByteBuffer heapBuffer = ByteBuffer.allocate(8192).alignedSlice(8);
    private final ByteBuffer nativeBuffer = ByteBuffer.allocateDirect(8192).alignedSlice(8);
    private final long[] values = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
    private final int valuesMask = values.length - 1;
    private final UnsafeBuffer agronaBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
    private int counter;

    @Setup(Level.Trial)
    public void setup()
    {
        LONG_ARRAY_VIEW
                .compareAndExchange(nativeBuffer, BUFFER_OFFSET, 0, values[0]);
        LONG_ARRAY_VIEW
                .compareAndExchange(heapBuffer, BUFFER_OFFSET, 0, values[0]);
        agronaBuffer.getAndSetLong(BUFFER_OFFSET, values[0]);
        counter = 1;
    }

    @Benchmark
    public long getAndAddLongNativeByteBuffer()
    {
        long nextValue = values[counter & valuesMask];
        counter++;
        return (long) LONG_ARRAY_VIEW
                .getAndAdd(nativeBuffer, BUFFER_OFFSET, nextValue);
    }

    @Benchmark
    public long getAndAddLongHeapByteBuffer()
    {
        long nextValue = values[counter & valuesMask];
        counter++;
        return (long) LONG_ARRAY_VIEW
                .getAndAdd(heapBuffer, BUFFER_OFFSET, nextValue);
    }

    @Benchmark
    public long getAndAddLongUnsafeBuffer()
    {
        long nextValue = values[counter & valuesMask];
        counter++;
        return agronaBuffer.getAndAddLong(BUFFER_OFFSET, nextValue);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = "-Dagrona.disable.bounds.checks=true")
    public long getAndAddLongUnsafeBufferWithoutBoundsCheck()
    {
        long nextValue = values[counter & valuesMask];
        counter++;
        return agronaBuffer.getAndAddLong(BUFFER_OFFSET, nextValue);
    }

    @Benchmark
    public long getLongVolatileNativeByteBuffer()
    {
        return (long) LONG_ARRAY_VIEW.getVolatile(nativeBuffer, BUFFER_OFFSET);
    }

    @Benchmark
    public long getLongVolatileHeapByteBuffer()
    {
        return (long) LONG_ARRAY_VIEW.getVolatile(heapBuffer, BUFFER_OFFSET);
    }

    @Benchmark
    public long getLongVolatileUnsafeBuffer()
    {
        return agronaBuffer.getLongVolatile(BUFFER_OFFSET);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = "-Dagrona.disable.bounds.checks=true")
    public long getLongVolatileUnsafeBufferWithoutBoundsCheck()
    {
        return agronaBuffer.getLongVolatile(BUFFER_OFFSET);
    }
}