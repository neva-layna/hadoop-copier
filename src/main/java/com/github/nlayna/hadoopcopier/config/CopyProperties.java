package com.github.nlayna.hadoopcopier.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "copy")
public class CopyProperties {
    private int threadPoolSize = 10;
}
