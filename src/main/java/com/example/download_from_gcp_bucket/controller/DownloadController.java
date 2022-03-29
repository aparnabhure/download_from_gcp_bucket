package com.example.download_from_gcp_bucket.controller;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

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

    @PostMapping(value = "/all", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<FileContent> downloadAll(@RequestBody final Request request) throws IOException {
        return ResponseEntity.ok(downloadAllFiles(request));
    }

    public FileContent downloadAllFiles(final Request request) throws IOException {
        GoogleCredentials googleCredentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(credential.getBytes()));
        Storage storage = StorageOptions.newBuilder().setCredentials(googleCredentials).build().getService();
//        Page<Blob> blobs =
//            storage.list(
//                bucketName,
//                Storage.BlobListOption.prefix(request.getDirectoryPrefix()),
//                Storage.BlobListOption.currentDirectory());

        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(request.getDirectoryPrefix()));
        byte[] decompressedBytes = null;
        String fileName = null;

        for (Blob blob : blobs.iterateAll()) {
            String name = blob.getName();
            if(name.endsWith(".csv.gz")) {
                fileName = blob.getName();
                byte[] contentBytes = blob.getContent();
                decompressedBytes = gzipUncompress(contentBytes);
               // blob.downloadTo(Paths.get(request.getOutputFileName()));
                break;
            }
        }

        FileContent fileContent = new FileContent();
        fileContent.setFileName(fileName);
        if(decompressedBytes == null){
            return fileContent;
        }

        //Parse the csv lines
        String data = new String(decompressedBytes, StandardCharsets.UTF_8);
        String[] lines = data.split("\n");
        List<Content> contentList = new ArrayList<>();
        for(int i=1; i< lines.length; i++){
            String line = lines[i];
            String[] values = line.split(",");
            String policyId = values[0].trim();
            String prevalence = values[1].trim();
            String actor = values[2].trim();
            String target = values[3].trim();
            int learningMC = Integer.parseInt(values[4].trim());
            int exhibitMC = Integer.parseInt(values[5].trim());
            int bashCOUNT = Integer.parseInt(values[6].trim());
            String virusId = values[7].trim();
            contentList.add(new Content(policyId, prevalence, actor, target, learningMC, exhibitMC, bashCOUNT, virusId));
        }

        fileContent.setContentList(contentList);
        return fileContent;
    }

    private byte[] gzipUncompress(byte[] compressedBytes) throws IOException {
        try (InputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes))) {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                inputStream.transferTo(outputStream);
                return outputStream.toByteArray();
            }
        }
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
        @JsonProperty("directory_prefix")
        String directoryPrefix;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    static
    class FileContent{
        @JsonProperty("name")
        String fileName;
        @JsonProperty("content")
        List<Content> contentList;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    static
    class Content{
        @JsonProperty("id")
        String pid;
        @JsonProperty("prevalence")
        String prevalence;
        @JsonProperty("actor")
        String actor;
        @JsonProperty("target")
        String target;
        @JsonProperty("lmc")
        int lmc;
        @JsonProperty("xmc")
        int xmc;
        @JsonProperty("bc")
        int bc;
        @JsonProperty("vid")
        String vid;
    }
}
