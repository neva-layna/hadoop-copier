package com.github.nlayna.hadoopcopier.model;

import lombok.Data;

import java.time.Instant;
import java.util.List;

@Data
public class CopyTask {
    private final String requestId;
    private final String namespace;
    private volatile CopyTaskStatus status = CopyTaskStatus.PENDING;
    private final List<CopyItemTask> items;
    private final Instant createdAt = Instant.now();
    private volatile Instant completedAt;

    public CopyTask(String requestId, String namespace, List<CopyItemTask> items) {
        this.requestId = requestId;
        this.namespace = namespace;
        this.items = items;
    }
}
