package com.aitusoftware.messaging.ipc;

import java.nio.ByteBuffer;

public interface IpcSubscriber
{
    void onMessage(ByteBuffer message);
}
