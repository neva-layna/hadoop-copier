package com.github.nlayna.hadoopcopier.service;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream wrapper that limits read throughput to a specified number of bytes per second.
 * Uses sleep-based throttling with a 1-second sliding window.
 */
public class ThrottledInputStream extends FilterInputStream {

    private final long maxBytesPerSecond;
    private long windowStartNanos;
    private long bytesReadInWindow;

    public ThrottledInputStream(InputStream in, long maxBytesPerSecond) {
        super(in);
        if (maxBytesPerSecond <= 0) {
            throw new IllegalArgumentException("maxBytesPerSecond must be positive, got: " + maxBytesPerSecond);
        }
        this.maxBytesPerSecond = maxBytesPerSecond;
        this.windowStartNanos = System.nanoTime();
        this.bytesReadInWindow = 0;
    }

    @Override
    public int read() throws IOException {
        throttle(1);
        int b = in.read();
        if (b != -1) {
            bytesReadInWindow++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        throttle(len);
        int bytesRead = in.read(b, off, len);
        if (bytesRead > 0) {
            bytesReadInWindow += bytesRead;
        }
        return bytesRead;
    }

    private void throttle(int bytesRequested) throws IOException {
        if (bytesReadInWindow >= maxBytesPerSecond) {
            long elapsedNanos = System.nanoTime() - windowStartNanos;
            long windowNanos = 1_000_000_000L;
            long sleepNanos = windowNanos - elapsedNanos;

            if (sleepNanos > 0) {
                try {
                    long sleepMs = sleepNanos / 1_000_000;
                    int sleepNanosRemainder = (int) (sleepNanos % 1_000_000);
                    Thread.sleep(sleepMs, sleepNanosRemainder);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Throttled read interrupted", e);
                }
            }

            // Reset window
            windowStartNanos = System.nanoTime();
            bytesReadInWindow = 0;
        }
    }
}
