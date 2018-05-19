package com.aitusoftware.messaging.benchmarks;

import com.aitusoftware.messaging.util.Affinity;

final class AffinityUtil
{
    private static final int AFFINITY = Integer.getInteger("bench.affinity", -1);

    static void set()
    {
        if (AFFINITY != -1)
        {
            new Affinity().setCurrentThreadCpuAffinityAndValidate(AFFINITY);
        }
    }
}
