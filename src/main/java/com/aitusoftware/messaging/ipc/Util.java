package com.aitusoftware.messaging.ipc;

final class Util {
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

}
