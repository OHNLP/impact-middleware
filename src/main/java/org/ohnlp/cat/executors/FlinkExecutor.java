package org.ohnlp.cat.executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.ohnlp.cat.api.criteria.Criterion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.io.File;
import java.util.Locale;
import java.util.UUID;


@Component
public class FlinkExecutor implements JobExecutor {

    private final ExecutorConfig config;
    private final RestTemplate flink;
    private final String backendFlinkJarUID;

    @Autowired
    public FlinkExecutor(ExecutorConfig config) {
        this.config = config;
        this.flink = new RestTemplate();
        flink.setUriTemplateHandler(new DefaultUriBuilderFactory(config.jobManagerURI));
        if (config.uploadJarFromLocal) {
            backendFlinkJarUID = uploadJobJar();
        } else {
            backendFlinkJarUID = config.getJarUID();
        }
    }

    private String uploadJobJar() {
        // Upload the JAR to cluster. Flink requires the application/x-java-archive header
        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(new MediaType("application", "x-java-archive"));
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        MultiValueMap<String, Object> body
                = new LinkedMultiValueMap<>();
        body.add("file", new FileSystemResource(new File(config.backendJarPath)));
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);
        JsonNode jarInfo = this.flink.postForObject("/jars/upload", requestEntity, JsonNode.class);
        if (jarInfo != null && jarInfo.has("filename")) {
            return jarInfo.get("filename").asText(); // filename is the JAR ID for flink API calls
        } else {
            throw new IllegalArgumentException("Job Jar Upload to Flink Cluster Failed!");
        }
    }

    @Override
    public String startJob(UUID jobUID, Criterion criterion, String callbackURL) throws Exception {
        ObjectNode requestBody = JsonNodeFactory.instance.objectNode();
        requestBody.put("entryClass", "org.ohnlp.ir.cat.CohortIdentificationJob");
        requestBody.put("programArgs", String.join(",",
                        "--runner=FlinkRunner",
                        "--callback=" + callbackURL,
                        "--jobid=" + jobUID.toString().toLowerCase(Locale.ROOT)
                )
        );
        requestBody.put("parallelism", config.getJobParallelism());
        JsonNode job = flink.postForObject("/jars/" + backendFlinkJarUID + "/run", requestBody, JsonNode.class);
        if (job == null || !job.has("jobid")) {
            throw new Exception("Job Start on Flink Cluster Failed!");
        } else {
            return job.get("jobid").asText();
        }
    }

    @Configuration
    @ConfigurationProperties("flink")
    public static class ExecutorConfig {
        private String jobManagerURI;
        private boolean uploadJarFromLocal;
        private String jarUID;
        private String backendJarPath;
        private int jobParallelism;

        public String getJobManagerURI() {
            return jobManagerURI;
        }

        public void setJobManagerURI(String jobManagerURI) {
            this.jobManagerURI = jobManagerURI;
        }

        public boolean isUploadJarFromLocal() {
            return uploadJarFromLocal;
        }

        public void setUploadJarFromLocal(boolean uploadJarFromLocal) {
            this.uploadJarFromLocal = uploadJarFromLocal;
        }

        public String getJarUID() {
            return jarUID;
        }

        public void setJarUID(String jarUID) {
            this.jarUID = jarUID;
        }

        public String getBackendJarPath() {
            return backendJarPath;
        }

        public void setBackendJarPath(String backendJarPath) {
            this.backendJarPath = backendJarPath;
        }

        public int getJobParallelism() {
            return jobParallelism;
        }

        public void setJobParallelism(int jobParallelism) {
            this.jobParallelism = jobParallelism;
        }
    }
}
