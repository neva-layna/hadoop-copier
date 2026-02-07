package com.github.nlayna.hadoopcopier.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

@Slf4j
@Service
public class HdfsCopyService {

    public long copyPath(FileSystem fs, String hdfsPath, String localPath) throws IOException {
        Path sourcePath = new Path(hdfsPath);

        if (!fs.exists(sourcePath)) {
            throw new IOException("Source path does not exist: " + hdfsPath);
        }

        FileStatus sourceStatus = fs.getFileStatus(sourcePath);
        if (sourceStatus.isDirectory()) {
            return copyDirectory(fs, sourcePath, localPath);
        } else {
            return copyFile(fs, sourcePath, localPath, sourceStatus.getLen());
        }
    }

    private long copyFile(FileSystem fs, Path sourcePath, String localPath, long fileSize) throws IOException {
        log.info("Copying file {} -> {} (size: {} bytes)", sourcePath, localPath, fileSize);

        boolean success = FileUtil.copy(fs, sourcePath, new File(localPath), false, fs.getConf());
        if (!success) {
            throw new IOException("Failed to copy file: " + sourcePath);
        }

        return fileSize;
    }

    private long copyDirectory(FileSystem fs, Path sourcePath, String localPath) throws IOException {
        log.info("Copying directory {} -> {}", sourcePath, localPath);

        File localDir = new File(localPath);
        if (!localDir.exists() && !localDir.mkdirs()) {
            throw new IOException("Failed to create local directory: " + localPath);
        }

        Configuration conf = fs.getConf();
        boolean copySuccess = false;
        try {
            copySuccess = FileUtil.copy(fs, sourcePath, localDir, false, conf);
        } catch (IOException e) {
            log.warn("FileUtil.copy failed for {}, falling back to manual copy: {}", sourcePath, e.getMessage());
        }

        if (!copySuccess) {
            log.info("Using manual copy for directory: {}", sourcePath);
            return manualCopyDirectory(fs, sourcePath, localDir, conf);
        }

        return calculateDirectorySize(fs, sourcePath);
    }

    private long manualCopyDirectory(FileSystem fs, Path sourcePath, File localDir, Configuration conf) throws IOException {
        Stack<Path> dirsToProcess = new Stack<>();
        dirsToProcess.push(sourcePath);

        Map<Path, File> pathMap = new HashMap<>();
        pathMap.put(sourcePath, localDir);

        long totalBytes = 0;
        int filesCopied = 0;
        int dirsCopied = 0;

        while (!dirsToProcess.isEmpty()) {
            Path currentDir = dirsToProcess.pop();
            File localCurrentDir = pathMap.get(currentDir);

            FileStatus[] items = fs.listStatus(currentDir);
            for (FileStatus item : items) {
                Path itemPath = item.getPath();
                File localItem = new File(localCurrentDir, itemPath.getName());

                if (item.isDirectory()) {
                    if (!localItem.exists() && !localItem.mkdirs()) {
                        throw new IOException("Failed to create directory: " + localItem.getAbsolutePath());
                    }
                    dirsCopied++;
                    dirsToProcess.push(itemPath);
                    pathMap.put(itemPath, localItem);
                } else {
                    boolean success = FileUtil.copy(fs, itemPath, localItem, false, conf);
                    if (!success) {
                        fs.copyToLocalFile(false, itemPath, new Path(localItem.getAbsolutePath()), true);
                    }
                    totalBytes += item.getLen();
                    filesCopied++;
                    log.debug("Copied file: {} ({} bytes)", itemPath.getName(), item.getLen());
                }
            }
        }

        log.info("Manual copy completed: {} files, {} directories", filesCopied, dirsCopied);
        return totalBytes;
    }

    private long calculateDirectorySize(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                size += calculateDirectorySize(fs, status.getPath());
            } else {
                size += status.getLen();
            }
        }
        return size;
    }
}
