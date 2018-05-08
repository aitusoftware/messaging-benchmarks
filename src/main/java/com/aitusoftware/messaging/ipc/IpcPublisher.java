package com.aitusoftware.messaging.ipc;

import java.nio.ByteBuffer;

public interface IpcPublisher
{
    int publish(ByteBuffer message);
}
