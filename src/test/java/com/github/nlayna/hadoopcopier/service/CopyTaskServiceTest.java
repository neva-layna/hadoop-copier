package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.model.*;
import com.github.nlayna.hadoopcopier.model.CopyResult;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CopyTaskServiceTest {

    @Mock
    private HdfsFileSystemFactory fileSystemFactory;

    @Mock
    private HdfsCopyService hdfsCopyService;

    @Mock
    private FileSystem fileSystem;

    private CopyTaskService copyTaskService;

    @BeforeEach
    void setUp() {
        copyTaskService = new CopyTaskService(
                fileSystemFactory,
                hdfsCopyService,
                Executors.newFixedThreadPool(4)
        );
    }

    @Test
    void submitTask_createsTaskAndReturnsId() {
        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");
        CopyItem item = new CopyItem();
        item.setHdfsPath("/data/result1");
        item.setLocalPath("/tmp/res1");
        request.setItems(List.of(item));

        String requestId = copyTaskService.submitTask(request);

        assertThat(requestId).isNotNull().isNotBlank();
        Optional<CopyTask> task = copyTaskService.getTask(requestId);
        assertThat(task).isPresent();
        assertThat(task.get().getNamespace()).isEqualTo("ns1");
        assertThat(task.get().getItems()).hasSize(1);
    }

    @Test
    void submitTask_successfulCopy_setsCompletedStatus() throws Exception {
        when(fileSystemFactory.createFileSystem("ns1")).thenReturn(fileSystem);
        when(hdfsCopyService.copyPath(eq(fileSystem), eq("/data/result1"), eq("/tmp/res1"), isNull()))
                .thenReturn(new CopyResult(1024L, true));

        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");
        CopyItem item = new CopyItem();
        item.setHdfsPath("/data/result1");
        item.setLocalPath("/tmp/res1");
        request.setItems(List.of(item));

        String requestId = copyTaskService.submitTask(request);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(CopyTaskStatus.COMPLETED);
            assertThat(task.getCompletedAt()).isNotNull();
            assertThat(task.getItems().get(0).getBytesCopied()).isEqualTo(1024L);
            assertThat(task.getItems().get(0).isChecksumVerified()).isTrue();
            assertThat(task.getItems().get(0).getStatus()).isEqualTo(CopyItemStatus.COMPLETED);
        });
    }

    @Test
    void submitTask_failedCopy_setsFailedStatus() throws Exception {
        when(fileSystemFactory.createFileSystem("ns1")).thenReturn(fileSystem);
        when(hdfsCopyService.copyPath(eq(fileSystem), anyString(), anyString(), isNull()))
                .thenThrow(new IOException("HDFS unavailable"));

        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");
        CopyItem item = new CopyItem();
        item.setHdfsPath("/data/result1");
        item.setLocalPath("/tmp/res1");
        request.setItems(List.of(item));

        String requestId = copyTaskService.submitTask(request);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(CopyTaskStatus.FAILED);
            assertThat(task.getItems().get(0).getStatus()).isEqualTo(CopyItemStatus.FAILED);
            assertThat(task.getItems().get(0).getErrorMessage()).isEqualTo("HDFS unavailable");
        });
    }

    @Test
    void submitTask_partialFailure_setsPartiallyFailedStatus() throws Exception {
        when(fileSystemFactory.createFileSystem("ns1")).thenReturn(fileSystem);
        when(hdfsCopyService.copyPath(eq(fileSystem), eq("/data/result1"), eq("/tmp/res1"), isNull()))
                .thenReturn(new CopyResult(1024L, true));
        when(hdfsCopyService.copyPath(eq(fileSystem), eq("/data/result2"), eq("/tmp/res2"), isNull()))
                .thenThrow(new IOException("File not found"));

        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");

        CopyItem item1 = new CopyItem();
        item1.setHdfsPath("/data/result1");
        item1.setLocalPath("/tmp/res1");

        CopyItem item2 = new CopyItem();
        item2.setHdfsPath("/data/result2");
        item2.setLocalPath("/tmp/res2");

        request.setItems(List.of(item1, item2));

        String requestId = copyTaskService.submitTask(request);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(CopyTaskStatus.PARTIALLY_FAILED);
        });
    }

    @Test
    void submitTask_multipleItems_executesInParallel() throws Exception {
        when(fileSystemFactory.createFileSystem("ns1")).thenReturn(fileSystem);
        when(hdfsCopyService.copyPath(eq(fileSystem), anyString(), anyString(), isNull()))
                .thenAnswer(invocation -> {
                    Thread.sleep(100);
                    return new CopyResult(512L, true);
                });

        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");

        CopyItem item1 = new CopyItem();
        item1.setHdfsPath("/data/r1");
        item1.setLocalPath("/tmp/r1");
        CopyItem item2 = new CopyItem();
        item2.setHdfsPath("/data/r2");
        item2.setLocalPath("/tmp/r2");
        CopyItem item3 = new CopyItem();
        item3.setHdfsPath("/data/r3");
        item3.setLocalPath("/tmp/r3");

        request.setItems(List.of(item1, item2, item3));

        String requestId = copyTaskService.submitTask(request);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(CopyTaskStatus.COMPLETED);
            assertThat(task.getItems()).allMatch(i -> i.getStatus() == CopyItemStatus.COMPLETED);
        });
    }

    @Test
    void getTask_nonExisting_returnsEmpty() {
        Optional<CopyTask> task = copyTaskService.getTask("non-existing-id");
        assertThat(task).isEmpty();
    }

    @Test
    void submitTask_setsInProgressStatusImmediately() {
        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");
        CopyItem item = new CopyItem();
        item.setHdfsPath("/data/result1");
        item.setLocalPath("/tmp/res1");
        request.setItems(List.of(item));

        String requestId = copyTaskService.submitTask(request);

        CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
        assertThat(task.getStatus()).isIn(CopyTaskStatus.IN_PROGRESS, CopyTaskStatus.COMPLETED, CopyTaskStatus.FAILED);
    }

    @Test
    void submitTask_withBandwidth_passesBandwidthToCopyService() throws Exception {
        when(fileSystemFactory.createFileSystem("ns1")).thenReturn(fileSystem);
        when(hdfsCopyService.copyPath(eq(fileSystem), eq("/data/result1"), eq("/tmp/res1"), eq(10)))
                .thenReturn(new CopyResult(2048L, true));

        CopyRequest request = new CopyRequest();
        request.setNamespace("ns1");
        request.setBandwidth(10);
        CopyItem item = new CopyItem();
        item.setHdfsPath("/data/result1");
        item.setLocalPath("/tmp/res1");
        request.setItems(List.of(item));

        String requestId = copyTaskService.submitTask(request);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            CopyTask task = copyTaskService.getTask(requestId).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(CopyTaskStatus.COMPLETED);
            assertThat(task.getBandwidth()).isEqualTo(10);
        });

        verify(hdfsCopyService).copyPath(eq(fileSystem), eq("/data/result1"), eq("/tmp/res1"), eq(10));
    }
}
