package com.github.nlayna.hadoopcopier.controller;

import com.github.nlayna.hadoopcopier.model.*;
import com.github.nlayna.hadoopcopier.service.CopyTaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(CopyController.class)
class CopyControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CopyTaskService copyTaskService;

    @Test
    void submitCopyRequest_validRequest_returns202() throws Exception {
        when(copyTaskService.submitTask(any())).thenReturn("test-request-id");

        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.requestId").value("test-request-id"));
    }

    @Test
    void submitCopyRequest_missingNamespace_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("namespace is required"));
    }

    @Test
    void submitCopyRequest_emptyNamespace_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "  ",
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("namespace is required"));
    }

    @Test
    void submitCopyRequest_emptyItems_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "items": []
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("items must not be empty"));
    }

    @Test
    void submitCopyRequest_missingItems_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("items must not be empty"));
    }

    @Test
    void submitCopyRequest_invalidJson_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid request body"));
    }

    @Test
    void getTaskStatus_existingTask_returns200() throws Exception {
        CopyTask task = new CopyTask("req-123", "ns1", null,
                List.of(new CopyItemTask("/data/res1", "/tmp/res1")));
        task.setStatus(CopyTaskStatus.IN_PROGRESS);

        when(copyTaskService.getTask("req-123")).thenReturn(Optional.of(task));

        mockMvc.perform(get("/api/v1/copy/req-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestId").value("req-123"))
                .andExpect(jsonPath("$.status").value("IN_PROGRESS"))
                .andExpect(jsonPath("$.items[0].hdfsPath").value("/data/res1"))
                .andExpect(jsonPath("$.items[0].checksumVerified").value(false));
    }

    @Test
    void getTaskStatus_completedWithChecksum_returnsChecksumVerified() throws Exception {
        CopyItemTask itemTask = new CopyItemTask("/data/res1", "/tmp/res1");
        itemTask.setStatus(CopyItemStatus.COMPLETED);
        itemTask.setBytesCopied(1024L);
        itemTask.setChecksumVerified(true);

        CopyTask task = new CopyTask("req-456", "ns1", null, List.of(itemTask));
        task.setStatus(CopyTaskStatus.COMPLETED);

        when(copyTaskService.getTask("req-456")).thenReturn(Optional.of(task));

        mockMvc.perform(get("/api/v1/copy/req-456"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].checksumVerified").value(true))
                .andExpect(jsonPath("$.items[0].bytesCopied").value(1024));
    }

    @Test
    void getTaskStatus_nonExistingTask_returns404() throws Exception {
        when(copyTaskService.getTask("unknown")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/copy/unknown"))
                .andExpect(status().isNotFound());
    }

    @Test
    void submitCopyRequest_multipleItems_returns202() throws Exception {
        when(copyTaskService.submitTask(any())).thenReturn("multi-id");

        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"},
                                        {"hdfsPath": "/data/result2", "localPath": "/tmp/res2"}
                                    ]
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.requestId").value("multi-id"));
    }

    @Test
    void submitCopyRequest_zeroBandwidth_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "bandwidth": 0,
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("bandwidth must be positive"));
    }

    @Test
    void submitCopyRequest_negativeBandwidth_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "bandwidth": -5,
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("bandwidth must be positive"));
    }

    @Test
    void submitCopyRequest_validBandwidth_returns202() throws Exception {
        when(copyTaskService.submitTask(any())).thenReturn("bw-request-id");

        mockMvc.perform(post("/api/v1/copy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "namespace": "nameservice1",
                                    "bandwidth": 10,
                                    "items": [
                                        {"hdfsPath": "/data/result1", "localPath": "/tmp/res1"}
                                    ]
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.requestId").value("bw-request-id"));
    }
}
