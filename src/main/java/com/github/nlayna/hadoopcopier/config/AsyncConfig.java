package com.github.nlayna.hadoopcopier.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@RequiredArgsConstructor
public class AsyncConfig {

    private final CopyProperties copyProperties;

    @Bean(name = "copyExecutor")
    public Executor copyExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(copyProperties.getThreadPoolSize());
        executor.setMaxPoolSize(copyProperties.getThreadPoolSize());
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("hdfs-copy-");
        executor.initialize();
        return executor;
    }
}
