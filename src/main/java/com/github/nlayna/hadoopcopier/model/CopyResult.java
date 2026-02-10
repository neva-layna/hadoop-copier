package com.github.nlayna.hadoopcopier.model;

public record CopyResult(long bytesCopied, boolean checksumVerified) {
}
