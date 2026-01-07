package com.doordash.spark.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Java client for the DCP Spark REST API.
 *
 * <p>This is the recommended way for Java services to submit Spark jobs to the Pedregal Spark Platform.
 *
 * <p>Example usage:
 * <pre>{@code
 * DCPSparkClient client = new DCPSparkClient("http://dcp-spark.service.prod.ddsd:8080");
 *
 * SubmitJobRequest request = SubmitJobRequest.builder()
 *     .name("my-etl-job")
 *     .team("data-infra")
 *     .user("user@doordash.com")
 *     .config(SparkJobConfig.builder()
 *         .sparkVersion("3.5")
 *         .jobType(JobType.BATCH)
 *         .coreEtl(CoreETLConfig.builder()
 *             .version("2.7.0")
 *             .jobSpec(jobSpecJson)
 *             .build())
 *         .resources(ResourceConfig.builder()
 *             .driver(DriverConfig.builder().cores(2).memory("4g").build())
 *             .executor(ExecutorConfig.builder().instances(5).cores(4).memory("8g").build())
 *             .build())
 *         .build())
 *     .build();
 *
 * SubmitJobResponse response = client.submitJob(request);
 * System.out.println("Job ID: " + response.getJobId());
 * }</pre>
 */
public class DCPSparkClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private String authToken;

    public DCPSparkClient(String baseUrl) {
        this(baseUrl, Duration.ofSeconds(30));
    }

    public DCPSparkClient(String baseUrl, Duration timeout) {
        this.baseUrl = baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    /**
     * Submits a new Spark job.
     */
    public SubmitJobResponse submitJob(SubmitJobRequest request) throws Exception {
        String body = objectMapper.writeValueAsString(request);

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));

        if (authToken != null) {
            reqBuilder.header("Authorization", "Bearer " + authToken);
        }

        HttpResponse<String> response = httpClient.send(
                reqBuilder.build(),
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200 && response.statusCode() != 202) {
            throw new RuntimeException("Failed to submit job: " + response.body());
        }

        return objectMapper.readValue(response.body(), SubmitJobResponse.class);
    }

    /**
     * Submits a job asynchronously.
     */
    public CompletableFuture<SubmitJobResponse> submitJobAsync(SubmitJobRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return submitJob(request);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Gets the status of a job.
     */
    public JobStatus getJobStatus(String jobId) throws Exception {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/jobs/" + jobId))
                .GET();

        if (authToken != null) {
            reqBuilder.header("Authorization", "Bearer " + authToken);
        }

        HttpResponse<String> response = httpClient.send(
                reqBuilder.build(),
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() == 404) {
            throw new JobNotFoundException("Job not found: " + jobId);
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to get job status: " + response.body());
        }

        return objectMapper.readValue(response.body(), JobStatus.class);
    }

    /**
     * Cancels a running job.
     */
    public void cancelJob(String jobId) throws Exception {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/jobs/" + jobId))
                .DELETE();

        if (authToken != null) {
            reqBuilder.header("Authorization", "Bearer " + authToken);
        }

        HttpResponse<String> response = httpClient.send(
                reqBuilder.build(),
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to cancel job: " + response.body());
        }
    }

    /**
     * Waits for a job to complete.
     */
    public JobStatus waitForCompletion(String jobId, Duration pollInterval, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);

        while (Instant.now().isBefore(deadline)) {
            JobStatus status = getJobStatus(jobId);

            if (status.isTerminal()) {
                return status;
            }

            Thread.sleep(pollInterval.toMillis());
        }

        throw new RuntimeException("Timeout waiting for job completion: " + jobId);
    }

    /**
     * Gets logs for a job.
     */
    public JobLogs getJobLogs(String jobId) throws Exception {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/jobs/" + jobId + "/logs"))
                .GET();

        if (authToken != null) {
            reqBuilder.header("Authorization", "Bearer " + authToken);
        }

        HttpResponse<String> response = httpClient.send(
                reqBuilder.build(),
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to get job logs: " + response.body());
        }

        return objectMapper.readValue(response.body(), JobLogs.class);
    }

    // ==================== Request/Response DTOs ====================

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SubmitJobRequest {
        private String name;
        private String team;
        private String user;
        private SparkJobConfig config;
        @JsonProperty("idempotency_key")
        private String idempotencyKey;
        @JsonProperty("callback_url")
        private String callbackUrl;

        // Builder pattern
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final SubmitJobRequest request = new SubmitJobRequest();

            public Builder name(String name) { request.name = name; return this; }
            public Builder team(String team) { request.team = team; return this; }
            public Builder user(String user) { request.user = user; return this; }
            public Builder config(SparkJobConfig config) { request.config = config; return this; }
            public Builder idempotencyKey(String key) { request.idempotencyKey = key; return this; }
            public Builder callbackUrl(String url) { request.callbackUrl = url; return this; }

            public SubmitJobRequest build() { return request; }
        }

        // Getters
        public String getName() { return name; }
        public String getTeam() { return team; }
        public String getUser() { return user; }
        public SparkJobConfig getConfig() { return config; }
        public String getIdempotencyKey() { return idempotencyKey; }
        public String getCallbackUrl() { return callbackUrl; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SparkJobConfig {
        @JsonProperty("spark_version")
        private String sparkVersion;
        @JsonProperty("job_type")
        private String jobType;
        @JsonProperty("core_etl")
        private CoreETLConfig coreEtl;
        @JsonProperty("custom_jar")
        private CustomJarConfig customJar;
        private ResourceConfig resources;
        @JsonProperty("spark_config")
        private Map<String, String> sparkConfig;
        @JsonProperty("env_vars")
        private Map<String, String> envVars;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final SparkJobConfig config = new SparkJobConfig();

            public Builder sparkVersion(String v) { config.sparkVersion = v; return this; }
            public Builder jobType(String t) { config.jobType = t; return this; }
            public Builder coreEtl(CoreETLConfig c) { config.coreEtl = c; return this; }
            public Builder customJar(CustomJarConfig c) { config.customJar = c; return this; }
            public Builder resources(ResourceConfig r) { config.resources = r; return this; }
            public Builder sparkConfig(Map<String, String> c) { config.sparkConfig = c; return this; }
            public Builder envVars(Map<String, String> e) { config.envVars = e; return this; }

            public SparkJobConfig build() { return config; }
        }

        // Getters
        public String getSparkVersion() { return sparkVersion; }
        public String getJobType() { return jobType; }
        public CoreETLConfig getCoreEtl() { return coreEtl; }
        public CustomJarConfig getCustomJar() { return customJar; }
        public ResourceConfig getResources() { return resources; }
        public Map<String, String> getSparkConfig() { return sparkConfig; }
        public Map<String, String> getEnvVars() { return envVars; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CoreETLConfig {
        private String version;
        @JsonProperty("job_spec")
        private String jobSpec;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final CoreETLConfig config = new CoreETLConfig();

            public Builder version(String v) { config.version = v; return this; }
            public Builder jobSpec(String s) { config.jobSpec = s; return this; }

            public CoreETLConfig build() { return config; }
        }

        public String getVersion() { return version; }
        public String getJobSpec() { return jobSpec; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CustomJarConfig {
        @JsonProperty("jar_path")
        private String jarPath;
        @JsonProperty("main_class")
        private String mainClass;
        private List<String> args;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final CustomJarConfig config = new CustomJarConfig();

            public Builder jarPath(String p) { config.jarPath = p; return this; }
            public Builder mainClass(String c) { config.mainClass = c; return this; }
            public Builder args(List<String> a) { config.args = a; return this; }

            public CustomJarConfig build() { return config; }
        }

        public String getJarPath() { return jarPath; }
        public String getMainClass() { return mainClass; }
        public List<String> getArgs() { return args; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ResourceConfig {
        private DriverConfig driver;
        private ExecutorConfig executor;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final ResourceConfig config = new ResourceConfig();

            public Builder driver(DriverConfig d) { config.driver = d; return this; }
            public Builder executor(ExecutorConfig e) { config.executor = e; return this; }

            public ResourceConfig build() { return config; }
        }

        public DriverConfig getDriver() { return driver; }
        public ExecutorConfig getExecutor() { return executor; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DriverConfig {
        private int cores;
        private String memory;
        @JsonProperty("memory_overhead")
        private String memoryOverhead;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final DriverConfig config = new DriverConfig();

            public Builder cores(int c) { config.cores = c; return this; }
            public Builder memory(String m) { config.memory = m; return this; }
            public Builder memoryOverhead(String o) { config.memoryOverhead = o; return this; }

            public DriverConfig build() { return config; }
        }

        public int getCores() { return cores; }
        public String getMemory() { return memory; }
        public String getMemoryOverhead() { return memoryOverhead; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ExecutorConfig {
        private int instances;
        private int cores;
        private String memory;
        @JsonProperty("memory_overhead")
        private String memoryOverhead;
        @JsonProperty("dynamic_allocation")
        private boolean dynamicAllocation;
        @JsonProperty("min_executors")
        private int minExecutors;
        @JsonProperty("max_executors")
        private int maxExecutors;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final ExecutorConfig config = new ExecutorConfig();

            public Builder instances(int i) { config.instances = i; return this; }
            public Builder cores(int c) { config.cores = c; return this; }
            public Builder memory(String m) { config.memory = m; return this; }
            public Builder memoryOverhead(String o) { config.memoryOverhead = o; return this; }
            public Builder dynamicAllocation(boolean d) { config.dynamicAllocation = d; return this; }
            public Builder minExecutors(int m) { config.minExecutors = m; return this; }
            public Builder maxExecutors(int m) { config.maxExecutors = m; return this; }

            public ExecutorConfig build() { return config; }
        }

        public int getInstances() { return instances; }
        public int getCores() { return cores; }
        public String getMemory() { return memory; }
        public String getMemoryOverhead() { return memoryOverhead; }
        public boolean isDynamicAllocation() { return dynamicAllocation; }
        public int getMinExecutors() { return minExecutors; }
        public int getMaxExecutors() { return maxExecutors; }
    }

    public static class SubmitJobResponse {
        @JsonProperty("job_id")
        private String jobId;
        @JsonProperty("execution_id")
        private String executionId;
        private String status;
        @JsonProperty("spark_ui_url")
        private String sparkUiUrl;
        private String message;

        public String getJobId() { return jobId; }
        public String getExecutionId() { return executionId; }
        public String getStatus() { return status; }
        public String getSparkUiUrl() { return sparkUiUrl; }
        public String getMessage() { return message; }
    }

    public static class JobStatus {
        @JsonProperty("job_id")
        private String jobId;
        @JsonProperty("execution_id")
        private String executionId;
        private String status;
        @JsonProperty("start_time")
        private Instant startTime;
        @JsonProperty("end_time")
        private Instant endTime;
        @JsonProperty("spark_ui_url")
        private String sparkUiUrl;
        @JsonProperty("logs_url")
        private String logsUrl;
        @JsonProperty("error_message")
        private String errorMessage;

        public String getJobId() { return jobId; }
        public String getExecutionId() { return executionId; }
        public String getStatus() { return status; }
        public Instant getStartTime() { return startTime; }
        public Instant getEndTime() { return endTime; }
        public String getSparkUiUrl() { return sparkUiUrl; }
        public String getLogsUrl() { return logsUrl; }
        public String getErrorMessage() { return errorMessage; }

        public boolean isTerminal() {
            return "SUCCEEDED".equals(status) ||
                   "FAILED".equals(status) ||
                   "CANCELLED".equals(status);
        }
    }

    public static class JobLogs {
        private String logs;
        @JsonProperty("logs_url")
        private String logsUrl;

        public String getLogs() { return logs; }
        public String getLogsUrl() { return logsUrl; }
    }

    public static class JobNotFoundException extends Exception {
        public JobNotFoundException(String message) {
            super(message);
        }
    }
}
