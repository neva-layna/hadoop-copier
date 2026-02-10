package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.config.CopyProperties;
import com.github.nlayna.hadoopcopier.model.CopyResult;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HdfsCopyServiceTest {

    @Mock
    private FileSystem fileSystem;

    private CopyProperties copyProperties;
    private HdfsCopyService hdfsCopyService;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() {
        copyProperties = new CopyProperties();
        copyProperties.setChecksumEnabled(true);
        hdfsCopyService = new HdfsCopyService(copyProperties);
    }

    private FSDataInputStream mockFsOpen(byte[] data) throws IOException {
        SeekableByteArrayInputStream seekable = new SeekableByteArrayInputStream(data);
        FSDataInputStream fsDataIn = new FSDataInputStream(seekable);
        when(fileSystem.open(any(Path.class))).thenReturn(fsDataIn);
        return fsDataIn;
    }

    @Test
    void copyPath_sourceNotExists_throwsException() throws Exception {
        Path sourcePath = new Path("/data/missing");
        when(fileSystem.exists(sourcePath)).thenReturn(false);

        assertThatThrownBy(() -> hdfsCopyService.copyPath(fileSystem, "/data/missing", "/tmp/dest", null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Source path does not exist");
    }

    @Test
    void copyPath_singleFile_copiesSuccessfully() throws Exception {
        Path sourcePath = new Path("/data/file.txt");
        byte[] fileContent = "hello world".getBytes();

        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);

        mockFsOpen(fileContent);

        String localDest = new File(tempDir, "file.txt").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/file.txt", localDest, null);

        assertThat(result.bytesCopied()).isEqualTo(fileContent.length);
        assertThat(result.checksumVerified()).isTrue();
        assertThat(new File(localDest)).exists();
        assertThat(Files.readAllBytes(new File(localDest).toPath())).isEqualTo(fileContent);
    }

    @Test
    void copyPath_singleFile_withBandwidth_copiesSuccessfully() throws Exception {
        Path sourcePath = new Path("/data/file.txt");
        byte[] fileContent = "hello world with bandwidth".getBytes();

        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);

        mockFsOpen(fileContent);

        String localDest = new File(tempDir, "file_bw.txt").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/file.txt", localDest, 10);

        assertThat(result.bytesCopied()).isEqualTo(fileContent.length);
        assertThat(result.checksumVerified()).isTrue();
        assertThat(new File(localDest)).exists();
        assertThat(Files.readAllBytes(new File(localDest).toPath())).isEqualTo(fileContent);
    }

    @Test
    void copyPath_directory_copiesSuccessfully() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path childPath = new Path("/data/dir/file1.txt");
        byte[] childContent = "child content".getBytes();
        FileStatus childFile = mock(FileStatus.class);
        when(childFile.isDirectory()).thenReturn(false);
        when(childFile.getLen()).thenReturn((long) childContent.length);
        when(childFile.getPath()).thenReturn(childPath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{childFile});

        mockFsOpen(childContent);

        String localDest = new File(tempDir, "dir").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest, null);

        assertThat(result.bytesCopied()).isEqualTo(childContent.length);
        assertThat(result.checksumVerified()).isTrue();
        assertThat(new File(localDest, "file1.txt")).exists();
    }

    @Test
    void copyPath_directory_withSubdirectory() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path subDirPath = new Path("/data/dir/subdir");
        FileStatus subDirStatus = mock(FileStatus.class);
        when(subDirStatus.isDirectory()).thenReturn(true);
        when(subDirStatus.getPath()).thenReturn(subDirPath);

        Path filePath = new Path("/data/dir/subdir/file.txt");
        byte[] fileContent = "nested file".getBytes();
        FileStatus fileInSubDir = mock(FileStatus.class);
        when(fileInSubDir.isDirectory()).thenReturn(false);
        when(fileInSubDir.getLen()).thenReturn((long) fileContent.length);
        when(fileInSubDir.getPath()).thenReturn(filePath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{subDirStatus});
        when(fileSystem.listStatus(subDirPath)).thenReturn(new FileStatus[]{fileInSubDir});

        mockFsOpen(fileContent);

        String localDest = new File(tempDir, "dir").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest, null);

        assertThat(result.bytesCopied()).isEqualTo(fileContent.length);
        assertThat(result.checksumVerified()).isTrue();
        assertThat(new File(localDest, "subdir/file.txt")).exists();
    }

    @Test
    void copyPath_directory_withBandwidth() throws Exception {
        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path childPath = new Path("/data/dir/file1.txt");
        byte[] childContent = "throttled content".getBytes();
        FileStatus childFile = mock(FileStatus.class);
        when(childFile.isDirectory()).thenReturn(false);
        when(childFile.getLen()).thenReturn((long) childContent.length);
        when(childFile.getPath()).thenReturn(childPath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{childFile});

        mockFsOpen(childContent);

        String localDest = new File(tempDir, "dir_bw").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest, 5);

        assertThat(result.bytesCopied()).isEqualTo(childContent.length);
        assertThat(result.checksumVerified()).isTrue();
    }

    @Test
    void copyPath_checksumDisabled_returnsNotVerified() throws Exception {
        copyProperties.setChecksumEnabled(false);

        Path sourcePath = new Path("/data/file.txt");
        byte[] fileContent = "no checksum".getBytes();

        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);

        mockFsOpen(fileContent);

        String localDest = new File(tempDir, "file_nochk.txt").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/file.txt", localDest, null);

        assertThat(result.bytesCopied()).isEqualTo(fileContent.length);
        assertThat(result.checksumVerified()).isFalse();
        assertThat(Files.readAllBytes(new File(localDest).toPath())).isEqualTo(fileContent);
    }

    @Test
    void copyPath_checksumMismatch_throwsException() throws Exception {
        Path sourcePath = new Path("/data/file.txt");
        byte[] fileContent = "test data".getBytes();

        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(fileStatus);

        mockFsOpen(fileContent);

        String localDest = new File(tempDir, "file_mismatch.txt").getAbsolutePath();

        HdfsCopyService spyService = spy(hdfsCopyService);
        doReturn(new byte[]{0x00, 0x01, 0x02}).when(spyService).computeLocalFileMd5(any(File.class));

        assertThatThrownBy(() -> spyService.copyPath(fileSystem, "/data/file.txt", localDest, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Checksum mismatch");
    }

    @Test
    void copyPath_directory_checksumDisabled_returnsNotVerified() throws Exception {
        copyProperties.setChecksumEnabled(false);

        Path sourcePath = new Path("/data/dir");
        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);

        Path childPath = new Path("/data/dir/file1.txt");
        byte[] childContent = "dir content".getBytes();
        FileStatus childFile = mock(FileStatus.class);
        when(childFile.isDirectory()).thenReturn(false);
        when(childFile.getLen()).thenReturn((long) childContent.length);
        when(childFile.getPath()).thenReturn(childPath);

        when(fileSystem.exists(sourcePath)).thenReturn(true);
        when(fileSystem.getFileStatus(sourcePath)).thenReturn(dirStatus);
        when(fileSystem.listStatus(sourcePath)).thenReturn(new FileStatus[]{childFile});

        mockFsOpen(childContent);

        String localDest = new File(tempDir, "dir_nochk").getAbsolutePath();
        CopyResult result = hdfsCopyService.copyPath(fileSystem, "/data/dir", localDest, null);

        assertThat(result.bytesCopied()).isEqualTo(childContent.length);
        assertThat(result.checksumVerified()).isFalse();
    }

    /**
     * A seekable InputStream backed by a byte array, implementing the interfaces
     * required by FSDataInputStream.
     */
    private static class SeekableByteArrayInputStream extends ByteArrayInputStream
            implements org.apache.hadoop.fs.Seekable, org.apache.hadoop.fs.PositionedReadable {

        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > count) {
                throw new IOException("Invalid seek position: " + pos);
            }
            this.pos = (int) pos;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) {
            if (position >= count) return -1;
            int available = count - (int) position;
            int toRead = Math.min(length, available);
            System.arraycopy(buf, (int) position, buffer, offset, toRead);
            return toRead;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            int bytesRead = read(position, buffer, offset, length);
            if (bytesRead < length) {
                throw new IOException("Not enough data");
            }
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }
    }
}
