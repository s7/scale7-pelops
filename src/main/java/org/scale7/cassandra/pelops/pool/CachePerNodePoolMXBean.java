package org.scale7.cassandra.pelops.pool;

import java.util.concurrent.atomic.AtomicLong;

public interface CachePerNodePoolMXBean {
    String JMX_MBEAN_OBJ_NAME = "com.scale7.cassandra.pelops:type=CachePerNodePool";

    long getGetConnCount();

    long getLeastLoadedSelectedCount();

    long getLeastLoadedNotSelectedCount();

    long getCacheConnNotSelectedCount();

    long getCacheConnSelectedCount();

    long getConnSelectedAlreadyOpenCount();

    long getConnSelectedNotAlreadyOpenCount();

    long getConnCannotOpenCount();

    long getConnClosedCount();

    long getConnCreatedCount();

    long getConnReleaseCalledCount();

    long getConnAddToCacheCount();

    long getConnCreateExceptionCount();

    long getConnOpenedCount();

    long getConnReturnedToCacheCount();

    long getDeadConnCount();

    long getNetworkExceptionCount();

    long getPurgeAllSessionConnsCount();

    long getRefillBackoffCount();

    long getRefillNeedConnCount();

    long getGetConnBackoffCount();

}
