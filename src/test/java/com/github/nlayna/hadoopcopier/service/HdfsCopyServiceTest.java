package com.github.nlayna.hadoopcopier.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HdfsCopyServiceTest {

    @Mock
    private FileSystem fileSystem;

    @Mock
    private Configuration configuration;

    private HdfsCopyService hdfsCopyService;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() {
        hdfsCopyService = new HdfsCopyService();
    }

    @Test
    void copyPath_sourceNotExists_throwsException() throws Exception {
        Path sourcePath = new Path("/data/missing");
        when(fileSystem.exists(sourcePath)).thenReturn(false);

        assertThatThrownBy(() -> hdfsCopyService.copyPath(fileSystem, "/data/missing", "/tmp/dest"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Source path does not exist");
    }

    @Test
    void copyPath_singleFile_copiesSuccessfully() throws Exception {
        Path sourcePath = new Path("/data/file.txt");
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(2048L);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);
        when(fileSystem.getConf()).thenReturn(configuration);

        String localDest = new File(tempDir, "file.txt").getAbsolutePath();

        try (MockedStatic<FileUtil> fileUtilMock = mockStatic(FileUtil.class)) {
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(sourcePath), any(File.class), eq(false), eq(configuration)))
                    .thenReturn(true);

            long bytes = hdfsCopyService.copyPath(fileSystem, "/data/file.txt", localDest);
            assertThat(bytes).isEqualTo(2048L);
        }
    }

    @Test
    void copyPath_singleFile_copyFails_throwsException() throws Exception {
        Path sourcePath = new Path("/data/file.txt");
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(2048L);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);
        when(fileSystem.getConf()).thenReturn(configuration);

        String localDest = new File(tempDir, "file.txt").getAbsolutePath();

        try (MockedStatic<FileUtil> fileUtilMock = mockStatic(FileUtil.class)) {
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(sourcePath), any(File.class), eq(false), eq(configuration)))
                    .thenReturn(false);

            assertThatThrownBy(() -> hdfsCopyService.copyPath(fileSystem, "/data/file.txt", localDest))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to copy file");
        }
    }

    @Test
    void copyPath_directory_copiesSuccessfully() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        FileStatus childFile = mock(FileStatus.class);
        when(childFile.isDirectory()).thenReturn(false);
        when(childFile.getLen()).thenReturn(1024L);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{childFile});

        String localDest = new File(tempDir, "dir").getAbsolutePath();

        try (MockedStatic<FileUtil> fileUtilMock = mockStatic(FileUtil.class)) {
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(sourcePath), any(File.class), eq(false), eq(configuration)))
                    .thenReturn(true);

            long bytes = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest);
            assertThat(bytes).isEqualTo(1024L);
        }
    }

    @Test
    void copyPath_directory_fileUtilFails_fallsBackToManualCopy() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path childPath = new Path("/data/dir/file1.txt");
        FileStatus childFile = mock(FileStatus.class);
        when(childFile.isDirectory()).thenReturn(false);
        when(childFile.getLen()).thenReturn(512L);
        when(childFile.getPath()).thenReturn(childPath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{childFile});

        String localDest = new File(tempDir, "dir").getAbsolutePath();

        try (MockedStatic<FileUtil> fileUtilMock = mockStatic(FileUtil.class)) {
            // First call (directory copy) throws exception
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(sourcePath), any(File.class), eq(false), eq(configuration)))
                    .thenThrow(new IOException("Copy failed"));

            // Second call (manual file copy) succeeds
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(childPath), any(File.class), eq(false), eq(configuration)))
                    .thenReturn(true);

            long bytes = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest);
            assertThat(bytes).isEqualTo(512L);
        }
    }

    @Test
    void copyPath_directory_manualCopyWithSubdirectory() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path subDirPath = new Path("/data/dir/subdir");
        FileStatus subDirStatus = mock(FileStatus.class);
        when(subDirStatus.isDirectory()).thenReturn(true);
        when(subDirStatus.getPath()).thenReturn(subDirPath);

        Path filePath = new Path("/data/dir/subdir/file.txt");
        FileStatus fileInSubDir = mock(FileStatus.class);
        when(fileInSubDir.isDirectory()).thenReturn(false);
        when(fileInSubDir.getLen()).thenReturn(256L);
        when(fileInSubDir.getPath()).thenReturn(filePath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{subDirStatus});
        when(fileSystem.listStatus(subDirPath)).thenReturn(new FileStatus[]{fileInSubDir});

        String localDest = new File(tempDir, "dir").getAbsolutePath();

        try (MockedStatic<FileUtil> fileUtilMock = mockStatic(FileUtil.class)) {
            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(sourcePath), any(File.class), eq(false), eq(configuration)))
                    .thenThrow(new IOException("Copy failed"));

            fileUtilMock.when(() -> FileUtil.copy(
                    eq(fileSystem), eq(filePath), any(File.class), eq(false), eq(configuration)))
                    .thenReturn(true);

            long bytes = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest);
            assertThat(bytes).isEqualTo(256L);
        }
    }
}
