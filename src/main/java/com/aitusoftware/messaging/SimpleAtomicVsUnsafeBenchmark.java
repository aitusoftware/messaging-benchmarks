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

import org.agrona.UnsafeAccess;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import sun.misc.Unsafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

@State(Scope.Benchmark)
@Measurement(iterations = 10)
@Warmup(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
public class SimpleAtomicVsUnsafeBenchmark
{
    private final AtomicLong atomic = new AtomicLong();
    private final LongContainer updater = new LongContainer();
    private final UnsafeContainer unsafe = new UnsafeContainer();

    private static final class UnsafeContainer {
        private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
        private static final long OFFSET;
        static
        {
            try
            {
                OFFSET = UNSAFE.objectFieldOffset(UnsafeContainer.class.getDeclaredField("value"));
            }
            catch (NoSuchFieldException e)
            {
                throw new IllegalStateException();
            }
        }

        private volatile long value;

        public boolean compareAndSet(long expect, long value) {
            return UNSAFE.compareAndSwapLong(this, OFFSET, expect, value);
        }

        public long get() {
            return UNSAFE.getLongVolatile(this, OFFSET);
        }

        public void lazySet(long value) {
            UNSAFE.putOrderedLong(this, OFFSET, value);
        }

        public void setVolatile(long value) {
            UNSAFE.putLong(this, OFFSET, value);
        }

    }

    private static final class LongContainer {
        private static final AtomicLongFieldUpdater<LongContainer> UPDATER =
                AtomicLongFieldUpdater.newUpdater(LongContainer.class, "value");
        private volatile long value;

        public boolean compareAndSet(long expect, long value) {
            return UPDATER.compareAndSet(this, expect, value);
        }

        public long get() {
            return UPDATER.get(this);
        }

        public void lazySet(long value) {
            UPDATER.lazySet(this, value);
        }

        public void setVolatile(long value) {
            UPDATER.set(this, value);
        }
    }

    private final long[] values = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
    private final int valuesMask = values.length - 1;
    private int counter;

    @Setup(Level.Trial)
    public void setup()
    {
        atomic.set(values[0]);
        updater.setVolatile(values[0]);
        unsafe.setVolatile(values[0]);
        counter = 1;
    }

    @TearDown(Level.Iteration)
    public void tearDown(Blackhole bh) {
        bh.consume(atomic.get());
        bh.consume(updater.get());
        bh.consume(unsafe.get());
    }

    @Benchmark
    public long atomicGetVolatile()
    {
        return atomic.get();
    }

    @Benchmark
    public long updaterGetVolatile() {
        return updater.get();
    }

    @Benchmark
    public long unsafeGetVolatile() {
        return unsafe.get();
    }

    @Benchmark
    public boolean atomicCompareAndSet() {
        long nextValue = values[counter & valuesMask];
        long previousValue = values[(counter - 1) & valuesMask];
        counter++;

        return atomic.compareAndSet(previousValue, nextValue);
    }

    @Benchmark
    public boolean updaterCompareAndSet() {
        long nextValue = values[counter & valuesMask];
        long previousValue = values[(counter - 1) & valuesMask];
        counter++;

        return updater.compareAndSet(previousValue, nextValue);
    }

    @Benchmark
    public boolean unsafeCompareAndSet() {
        long nextValue = values[counter & valuesMask];
        long previousValue = values[(counter - 1) & valuesMask];
        counter++;

        return unsafe.compareAndSet(previousValue, nextValue);
    }

    @Benchmark
    public void atomicLazySet()
    {
        atomic.lazySet(values[counter & valuesMask]);
        counter++;
    }

    @Benchmark
    public void updaterLazySet()
    {
        updater.lazySet(values[counter & valuesMask]);
        counter++;
    }

    @Benchmark
    public void unsafeLazySet()
    {
        unsafe.lazySet(values[counter & valuesMask]);
        counter++;
    }
}