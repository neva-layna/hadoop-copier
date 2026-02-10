package com.github.nlayna.hadoopcopier.model;

import lombok.Data;

@Data
public class CopyItemTask {
    private String hdfsPath;
    private String localPath;
    private volatile CopyItemStatus status = CopyItemStatus.PENDING;
    private volatile long bytesCopied;
    private volatile long durationMs;
    private volatile String errorMessage;
    private volatile boolean checksumVerified;

    public CopyItemTask(String hdfsPath, String localPath) {
        this.hdfsPath = hdfsPath;
        this.localPath = localPath;
    }

    public String getSpeed() {
        if (durationMs <= 0 || bytesCopied <= 0) {
            return "N/A";
        }
        double seconds = durationMs / 1000.0;
        double mbPerSec = (bytesCopied / (1024.0 * 1024.0)) / seconds;
        return String.format("%.2f MB/s", mbPerSec);
    }
}
