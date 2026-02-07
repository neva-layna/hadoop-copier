package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.config.HadoopProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.hadoop.security.UserGroupInformation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class HdfsFileSystemFactoryTest {

    private HdfsFileSystemFactory factory;
    private HadoopProperties hadoopProperties;

    @BeforeEach
    void setUp() {
        hadoopProperties = new HadoopProperties();
        hadoopProperties.setConfBasedir("/test/hadoop/conf");
        factory = new HdfsFileSystemFactory(hadoopProperties);
    }

    @Test
    void getConfiguration_createsConfigWithCorrectResources() {
        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any())).thenAnswer(i -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromSubject(null)).thenAnswer(i -> null);

            Configuration conf = factory.getConfiguration("ns1");

            assertThat(conf).isNotNull();
            assertThat(conf.get("fs.file.impl")).isEqualTo("org.apache.hadoop.fs.LocalFileSystem");
            assertThat(conf.get("fs.hdfs.impl")).isEqualTo("org.apache.hadoop.hdfs.DistributedFileSystem");
            assertThat(conf.get("hadoop.security.authentication")).isEqualTo("kerberos");
            assertThat(conf.getBoolean("fs.hdfs.impl.disable.cache", false)).isTrue();
        }
    }

    @Test
    void getConfiguration_cachesPerNamespace() {
        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any())).thenAnswer(i -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromSubject(null)).thenAnswer(i -> null);

            Configuration conf1 = factory.getConfiguration("ns1");
            Configuration conf2 = factory.getConfiguration("ns1");

            assertThat(conf1).isSameAs(conf2);
        }
    }

    @Test
    void getConfiguration_differentNamespaces_differentConfigs() {
        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any())).thenAnswer(i -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromSubject(null)).thenAnswer(i -> null);

            Configuration conf1 = factory.getConfiguration("ns1");
            Configuration conf2 = factory.getConfiguration("ns2");

            assertThat(conf1).isNotSameAs(conf2);
        }
    }
}
