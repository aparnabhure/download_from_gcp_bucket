package com.example.download_from_gcp_bucket.controller;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

@Slf4j
@Controller
@RequestMapping("/download")
public class DownloadController {

    @Value("${storage.full.access.key}")
    private String credential;

    @Value("${storage.bucket.name}")
    private String bucketName;


    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<String> download(@RequestBody final Request request) throws IOException {
        return ResponseEntity.ok(downloadFile(request));
    }

    public String downloadFile(final Request request) throws IOException {
        String fileContent = null;

        GoogleCredentials googleCredentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(credential.getBytes()));
        Storage storage = StorageOptions.newBuilder().setCredentials(googleCredentials).build().getService();
        BlobId blobId = BlobId.of(bucketName, request.getInputFilePath());
        Blob blob = storage.get(blobId);
        if(blob != null) {
            String filename = blob.getName();
            log.debug(filename);
            fileContent = new String(blob.getContent(), StandardCharsets.UTF_8);
            //Download locally just to see the content
            blob.downloadTo(Paths.get(request.getOutputFileName()));
        }

        return fileContent;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @Setter
    static
    class Request{
        @JsonProperty("input_file_path")
        String inputFilePath;
        @JsonProperty("output_file_name")
        String outputFileName;
    }
}
