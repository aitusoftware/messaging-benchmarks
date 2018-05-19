package com.aitusoftware.messaging.ipc;

import com.aitusoftware.messaging.util.Affinity;

final class Util {
    private static final int NO_CPU = -1;
    static final int PUBLISHER_CPU = Integer.getInteger("ipc.pub.cpu", NO_CPU);
    static final int ECHO_CPU = Integer.getInteger("ipc.echo.cpu", NO_CPU);
    static final int SUBSCRIBER_CPU = Integer.getInteger("ipc.sub.cpu", NO_CPU);


    static final int CACHE_LINE_SIZE_IN_BYTES = 64;
    static final int SUBSCRIBER_SEQUENCE_OFFSET = CACHE_LINE_SIZE_IN_BYTES + (8 * 7);
    static final int MESSAGE_HEADER_LENGTH = CACHE_LINE_SIZE_IN_BYTES;
    static final int DATA_OFFSET = CACHE_LINE_SIZE_IN_BYTES * 4;
    static final int PUBLISHER_SEQUENCE_OFFSET = 8 * 7;

    private static final int CACHE_LINE_SIZE_MASK = CACHE_LINE_SIZE_IN_BYTES - 1;

    static int padToCacheLine(int messageSize)
    {
        return messageSize + CACHE_LINE_SIZE_IN_BYTES -
                ((messageSize) & CACHE_LINE_SIZE_MASK);
    }

    static void setCpu(String name, int cpu)
    {
        if (cpu != NO_CPU)
        {
            System.out.printf("Setting CPU to %d for %s%n", cpu, name);
            new Affinity().setCurrentThreadCpuAffinityAndValidate(cpu);
        }
    }
}
