package com.github.nlayna.hadoopcopier.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CopyItemTaskTest {

    @Test
    void getSpeed_withValidData_returnsFormattedSpeed() {
        CopyItemTask task = new CopyItemTask("/hdfs/path", "/local/path");
        task.setBytesCopied(10 * 1024 * 1024L); // 10 MB
        task.setDurationMs(1000); // 1 second

        assertThat(task.getSpeed()).isEqualTo("10.00 MB/s");
    }

    @Test
    void getSpeed_zeroDuration_returnsNA() {
        CopyItemTask task = new CopyItemTask("/hdfs/path", "/local/path");
        task.setBytesCopied(1024L);
        task.setDurationMs(0);

        assertThat(task.getSpeed()).isEqualTo("N/A");
    }

    @Test
    void getSpeed_zeroBytes_returnsNA() {
        CopyItemTask task = new CopyItemTask("/hdfs/path", "/local/path");
        task.setBytesCopied(0);
        task.setDurationMs(1000);

        assertThat(task.getSpeed()).isEqualTo("N/A");
    }

    @Test
    void constructor_setsFieldsCorrectly() {
        CopyItemTask task = new CopyItemTask("/hdfs/path", "/local/path");

        assertThat(task.getHdfsPath()).isEqualTo("/hdfs/path");
        assertThat(task.getLocalPath()).isEqualTo("/local/path");
        assertThat(task.getStatus()).isEqualTo(CopyItemStatus.PENDING);
        assertThat(task.getBytesCopied()).isZero();
        assertThat(task.getDurationMs()).isZero();
        assertThat(task.getErrorMessage()).isNull();
    }
}
