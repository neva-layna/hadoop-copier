package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

@Slf4j
@Service
public class CopyTaskService {

    private final HdfsFileSystemFactory fileSystemFactory;
    private final HdfsCopyService hdfsCopyService;
    private final Executor copyExecutor;
    private final ConcurrentMap<String, CopyTask> tasks = new ConcurrentHashMap<>();

    public CopyTaskService(HdfsFileSystemFactory fileSystemFactory,
                           HdfsCopyService hdfsCopyService,
                           @Qualifier("copyExecutor") Executor copyExecutor) {
        this.fileSystemFactory = fileSystemFactory;
        this.hdfsCopyService = hdfsCopyService;
        this.copyExecutor = copyExecutor;
    }

    public String submitTask(CopyRequest request) {
        String requestId = UUID.randomUUID().toString();

        List<CopyItemTask> itemTasks = request.getItems().stream()
                .map(item -> new CopyItemTask(item.getHdfsPath(), item.getLocalPath()))
                .toList();

        CopyTask task = new CopyTask(requestId, request.getNamespace(), request.getBandwidth(), itemTasks);
        tasks.put(requestId, task);

        log.info("Task {} submitted: namespace={}, items={}", requestId, request.getNamespace(), itemTasks.size());

        executeTask(task);
        return requestId;
    }

    public Optional<CopyTask> getTask(String requestId) {
        return Optional.ofNullable(tasks.get(requestId));
    }

    private void executeTask(CopyTask task) {
        task.setStatus(CopyTaskStatus.IN_PROGRESS);
        CountDownLatch latch = new CountDownLatch(task.getItems().size());

        for (CopyItemTask itemTask : task.getItems()) {
            copyExecutor.execute(() -> {
                try {
                    executeItemCopy(task.getNamespace(), task.getBandwidth(), itemTask);
                } finally {
                    latch.countDown();
                }
            });
        }

        copyExecutor.execute(() -> {
            try {
                latch.await();
                finalizeTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Task {} interrupted while waiting for completion", task.getRequestId());
                task.setStatus(CopyTaskStatus.FAILED);
                task.setCompletedAt(Instant.now());
            }
        });
    }

    private void executeItemCopy(String namespace, Integer bandwidthMbPerSec, CopyItemTask itemTask) {
        itemTask.setStatus(CopyItemStatus.IN_PROGRESS);
        log.info("Copying: {} -> {}", itemTask.getHdfsPath(), itemTask.getLocalPath());

        long startTime = System.currentTimeMillis();
        try (FileSystem fs = fileSystemFactory.createFileSystem(namespace)) {
            CopyResult result = hdfsCopyService.copyPath(fs, itemTask.getHdfsPath(), itemTask.getLocalPath(), bandwidthMbPerSec);
            long duration = System.currentTimeMillis() - startTime;

            itemTask.setBytesCopied(result.bytesCopied());
            itemTask.setChecksumVerified(result.checksumVerified());
            itemTask.setDurationMs(duration);
            itemTask.setStatus(CopyItemStatus.COMPLETED);

            log.info("Completed: {} -> {} ({} bytes in {}ms, speed: {})",
                    itemTask.getHdfsPath(), itemTask.getLocalPath(),
                    result.bytesCopied(), duration, itemTask.getSpeed());
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            itemTask.setDurationMs(duration);
            itemTask.setStatus(CopyItemStatus.FAILED);
            itemTask.setErrorMessage(e.getMessage());
            log.error("Failed: {} -> {}: {}", itemTask.getHdfsPath(), itemTask.getLocalPath(), e.getMessage());
        }
    }

    private void finalizeTask(CopyTask task) {
        long completedCount = task.getItems().stream()
                .filter(i -> i.getStatus() == CopyItemStatus.COMPLETED)
                .count();
        long failedCount = task.getItems().stream()
                .filter(i -> i.getStatus() == CopyItemStatus.FAILED)
                .count();

        if (failedCount == 0) {
            task.setStatus(CopyTaskStatus.COMPLETED);
        } else if (completedCount == 0) {
            task.setStatus(CopyTaskStatus.FAILED);
        } else {
            task.setStatus(CopyTaskStatus.PARTIALLY_FAILED);
        }

        task.setCompletedAt(Instant.now());
        log.info("Task {} finished: status={}, completed={}, failed={}",
                task.getRequestId(), task.getStatus(), completedCount, failedCount);
    }
}
