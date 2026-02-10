package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.config.CopyProperties;
import com.github.nlayna.hadoopcopier.model.CopyResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

@Slf4j
@Service
@RequiredArgsConstructor
public class HdfsCopyService {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final CopyProperties copyProperties;

    public CopyResult copyPath(FileSystem fs, String hdfsPath, String localPath, Integer bandwidthMbPerSec) throws IOException {
        Path sourcePath = new Path(hdfsPath);

        if (!fs.exists(sourcePath)) {
            throw new IOException("Source path does not exist: " + hdfsPath);
        }

        FileStatus sourceStatus = fs.getFileStatus(sourcePath);
        if (sourceStatus.isDirectory()) {
            return copyDirectory(fs, sourcePath, localPath, bandwidthMbPerSec);
        } else {
            return copyFile(fs, sourcePath, localPath, bandwidthMbPerSec);
        }
    }

    private CopyResult copyFile(FileSystem fs, Path sourcePath, String localPath, Integer bandwidthMbPerSec) throws IOException {
        log.info("Copying file {} -> {}", sourcePath, localPath);

        File localFile = new File(localPath);
        File parentDir = localFile.getParentFile();
        if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
        }

        return copyWithStreams(fs, sourcePath, localFile, bandwidthMbPerSec);
    }

    private CopyResult copyDirectory(FileSystem fs, Path sourcePath, String localPath, Integer bandwidthMbPerSec) throws IOException {
        log.info("Copying directory {} -> {}", sourcePath, localPath);

        File localDir = new File(localPath);
        if (!localDir.exists() && !localDir.mkdirs()) {
            throw new IOException("Failed to create local directory: " + localPath);
        }

        return manualCopyDirectory(fs, sourcePath, localDir, bandwidthMbPerSec);
    }

    private CopyResult manualCopyDirectory(FileSystem fs, Path sourcePath, File localDir, Integer bandwidthMbPerSec) throws IOException {
        Stack<Path> dirsToProcess = new Stack<>();
        dirsToProcess.push(sourcePath);

        Map<Path, File> pathMap = new HashMap<>();
        pathMap.put(sourcePath, localDir);

        long totalBytes = 0;
        boolean allVerified = true;
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
                    CopyResult fileResult = copyWithStreams(fs, itemPath, localItem, bandwidthMbPerSec);
                    totalBytes += fileResult.bytesCopied();
                    if (!fileResult.checksumVerified()) {
                        allVerified = false;
                    }
                    filesCopied++;
                    log.debug("Copied file: {} ({} bytes)", itemPath.getName(), item.getLen());
                }
            }
        }

        log.info("Manual copy completed: {} files, {} directories", filesCopied, dirsCopied);
        return new CopyResult(totalBytes, allVerified && filesCopied > 0);
    }

    private CopyResult copyWithStreams(FileSystem fs, Path sourcePath, File localFile, Integer bandwidthMbPerSec) throws IOException {
        boolean checksumEnabled = copyProperties.isChecksumEnabled();
        long totalBytes = 0;

        MessageDigest sourceDigest = null;
        if (checksumEnabled) {
            try {
                sourceDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException("MD5 algorithm not available", e);
            }
        }

        try (InputStream rawIn = fs.open(sourcePath);
             InputStream throttledIn = wrapWithThrottle(rawIn, bandwidthMbPerSec);
             InputStream in = checksumEnabled ? new DigestInputStream(throttledIn, sourceDigest) : throttledIn;
             OutputStream out = new FileOutputStream(localFile)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
        }

        if (checksumEnabled) {
            byte[] sourceHash = sourceDigest.digest();
            byte[] localHash = computeLocalFileMd5(localFile);

            if (!MessageDigest.isEqual(sourceHash, localHash)) {
                throw new IOException("Checksum mismatch for " + localFile.getAbsolutePath()
                        + ": source=" + bytesToHex(sourceHash)
                        + ", local=" + bytesToHex(localHash));
            }
            log.debug("Checksum verified for {}: {}", localFile.getName(), bytesToHex(sourceHash));
            return new CopyResult(totalBytes, true);
        }

        return new CopyResult(totalBytes, false);
    }

    byte[] computeLocalFileMd5(File file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (InputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("MD5 algorithm not available", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private InputStream wrapWithThrottle(InputStream in, Integer bandwidthMbPerSec) {
        if (bandwidthMbPerSec == null) {
            return in;
        }
        long maxBytesPerSecond = (long) bandwidthMbPerSec * 1024 * 1024;
        return new ThrottledInputStream(in, maxBytesPerSecond);
    }
}
