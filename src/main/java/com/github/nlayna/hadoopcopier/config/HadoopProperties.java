package com.github.nlayna.hadoopcopier.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "hadoop")
public class HadoopProperties {
    private String confBasedir = "/etc/hadoop/conf";
}
