package com.github.nlayna.hadoopcopier.model;

import lombok.Data;

@Data
public class CopyItem {
    private String hdfsPath;
    private String localPath;
}
