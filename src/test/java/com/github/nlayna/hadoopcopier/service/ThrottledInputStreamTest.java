package com.github.nlayna.hadoopcopier.service;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrottledInputStreamTest {

    @Test
    void constructor_zeroBytesPerSecond_throwsException() {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        assertThatThrownBy(() -> new ThrottledInputStream(in, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytesPerSecond must be positive");
    }

    @Test
    void constructor_negativeBytesPerSecond_throwsException() {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        assertThatThrownBy(() -> new ThrottledInputStream(in, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytesPerSecond must be positive");
    }

    @Test
    void read_singleByte_returnsCorrectValue() throws IOException {
        byte[] data = {42, 13};
        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(data), 1024)) {
            assertThat(tis.read()).isEqualTo(42);
            assertThat(tis.read()).isEqualTo(13);
            assertThat(tis.read()).isEqualTo(-1);
        }
    }

    @Test
    void read_buffer_returnsCorrectData() throws IOException {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(data), 1024 * 1024)) {
            byte[] buffer = new byte[256];
            int bytesRead = tis.read(buffer, 0, buffer.length);
            assertThat(bytesRead).isEqualTo(256);
            assertThat(buffer).isEqualTo(data);
        }
    }

    @Test
    void read_emptyStream_returnsMinusOne() throws IOException {
        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(new byte[0]), 1024)) {
            assertThat(tis.read()).isEqualTo(-1);
        }
    }

    @Test
    void read_throttling_sleedsWhenLimitReached() throws IOException {
        // 100 bytes/sec limit, write 200 bytes — should take ~1 second
        byte[] data = new byte[200];
        long maxBytesPerSecond = 100;

        long start = System.nanoTime();
        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(data), maxBytesPerSecond)) {
            byte[] buffer = new byte[200];
            int total = 0;
            int bytesRead;
            while ((bytesRead = tis.read(buffer, 0, buffer.length)) != -1) {
                total += bytesRead;
            }
            assertThat(total).isEqualTo(200);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Should have slept at least ~800ms (allowing tolerance for scheduling)
        assertThat(elapsedMs).isGreaterThan(500);
    }

    @Test
    void read_interruptDuringThrottle_throwsIOException() throws Exception {
        byte[] data = new byte[200];
        long maxBytesPerSecond = 50;

        Thread testThread = Thread.currentThread();

        Thread interruptor = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            testThread.interrupt();
        });

        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(data), maxBytesPerSecond)) {
            interruptor.start();
            assertThatThrownBy(() -> {
                byte[] buffer = new byte[200];
                while (tis.read(buffer, 0, buffer.length) != -1) {
                    // keep reading until throttle sleeps and gets interrupted
                }
            }).isInstanceOf(IOException.class)
                    .hasMessageContaining("interrupted");
        } finally {
            interruptor.join();
            // Clear interrupted status
            Thread.interrupted();
        }
    }

    @Test
    void read_noThrottlingNeededWithinWindow() throws IOException {
        // High limit (10MB/s), small data — should complete fast without throttling
        byte[] data = new byte[1024];
        long maxBytesPerSecond = 10 * 1024 * 1024;

        long start = System.nanoTime();
        try (ThrottledInputStream tis = new ThrottledInputStream(new ByteArrayInputStream(data), maxBytesPerSecond)) {
            byte[] buffer = new byte[1024];
            int bytesRead = tis.read(buffer, 0, buffer.length);
            assertThat(bytesRead).isEqualTo(1024);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(500);
    }
}
