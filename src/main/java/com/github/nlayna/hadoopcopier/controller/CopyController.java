package com.github.nlayna.hadoopcopier.controller;

import com.github.nlayna.hadoopcopier.model.CopyRequest;
import com.github.nlayna.hadoopcopier.model.CopyTask;
import com.github.nlayna.hadoopcopier.service.CopyTaskService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/copy")
@RequiredArgsConstructor
public class CopyController {

    private final CopyTaskService copyTaskService;

    @PostMapping
    public ResponseEntity<Map<String, String>> submitCopyRequest(@RequestBody CopyRequest request) {
        if (request.getNamespace() == null || request.getNamespace().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "namespace is required"));
        }
        if (request.getItems() == null || request.getItems().isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "items must not be empty"));
        }

        String requestId = copyTaskService.submitTask(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("requestId", requestId));
    }

    @GetMapping("/{requestId}")
    public ResponseEntity<CopyTask> getTaskStatus(@PathVariable String requestId) {
        return copyTaskService.getTask(requestId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
