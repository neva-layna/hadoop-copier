package com.github.nlayna.hadoopcopier.model;

import lombok.Data;

import java.util.List;

@Data
public class CopyRequest {
    private String namespace;
    private List<CopyItem> items;
    private Integer bandwidth;
}
