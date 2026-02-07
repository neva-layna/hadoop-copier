package com.github.nlayna.hadoopcopier.service;

import com.github.nlayna.hadoopcopier.config.HadoopProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class HdfsFileSystemFactory {

    private final HadoopProperties hadoopProperties;
    private final ConcurrentMap<String, Configuration> configCache = new ConcurrentHashMap<>();

    public FileSystem createFileSystem(String namespace) throws IOException {
        Configuration conf = getConfiguration(namespace);
        return FileSystem.get(conf);
    }

    Configuration getConfiguration(String namespace) {
        return configCache.computeIfAbsent(namespace, this::buildConfiguration);
    }

    private Configuration buildConfiguration(String namespace) {
        Configuration configuration = new Configuration();
        String basedir = hadoopProperties.getConfBasedir();

        configuration.addResource(new Path(String.format("%s/%s/core-site.xml", basedir, namespace)));
        configuration.addResource(new Path(String.format("%s/%s/hdfs-site.xml", basedir, namespace)));

        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);

        String principal = System.getProperty("kerberos.principal");
        if (principal != null) {
            configuration.set("hadoop.kerberos.principal", principal);
        }

        try {
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromSubject(null);
        } catch (IOException e) {
            log.error("Kerberos authentication failed for namespace {}: {}", namespace, e.getMessage());
            throw new RuntimeException("Kerberos authentication failed", e);
        }

        log.info("Created Hadoop configuration for namespace: {}", namespace);
        return configuration;
    }
}
