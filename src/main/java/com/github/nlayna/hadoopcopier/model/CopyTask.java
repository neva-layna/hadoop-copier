package com.github.nlayna.hadoopcopier.model;

import lombok.Data;

import java.time.Instant;
import java.util.List;

@Data
public class CopyTask {
    private final String requestId;
    private final String namespace;
    private final Integer bandwidth;
    private volatile CopyTaskStatus status = CopyTaskStatus.PENDING;
    private final List<CopyItemTask> items;
    private final Instant createdAt = Instant.now();
    private volatile Instant completedAt;

    public CopyTask(String requestId, String namespace, Integer bandwidth, List<CopyItemTask> items) {
        this.requestId = requestId;
        this.namespace = namespace;
        this.bandwidth = bandwidth;
        this.items = items;
    }
}
