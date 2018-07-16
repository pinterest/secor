package com.pinterest.secor.uploader;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.util.concurrent.RateLimiter;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Manages uploads to Google Cloud Storage using the Storage class from the Google API SDK.
 * <p>
 * It will use Service Account credential (json file) that can be generated from the Google Developers Console.
 * By default it will look up configured credential path in secor.gs.credentials.path or fallback to the default
 * credential in the environment variable GOOGLE_APPLICATION_CREDENTIALS.
 * <p>
 * Application credentials documentation
 * https://developers.google.com/identity/protocols/application-default-credentials
 *
 * @author Jerome Gagnon (jerome.gagnon.1@gmail.com)
 */
public class GsUploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(GsUploadManager.class);

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private final ExecutorService executor;

    protected RateLimiter rateLimiter;

    /**
     * Global instance of the Storage. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static Storage mStorageService;

    private Storage mClient;

    public GsUploadManager(SecorConfig config) throws Exception {
        super(config);
        executor = Executors.newFixedThreadPool(mConfig.getGsThreadPoolSize());
        rateLimiter = RateLimiter.create(mConfig.getGsRateLimit());

        mClient = getService(mConfig.getGsCredentialsPath(),
                mConfig.getGsConnectTimeoutInMs(), mConfig.getGsReadTimeoutInMs());
    }

    @Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        final String gsBucket = mConfig.getGsBucket();
        final String gsKey = localPath.withPrefix(mConfig.getGsPath()).getLogFilePath();
        final File localFile = new File(localPath.getLogFilePath());
        final boolean directUpload = mConfig.getGsDirectUpload();

        LOG.info("uploading file {} to gs://{}/{}", localFile, gsBucket, gsKey);

        final BlobInfo sourceBlob = BlobInfo
            .newBuilder(BlobId.of(gsBucket, gsKey))
            .setContentType(Files.probeContentType(localFile.toPath()))
            .build();

        rateLimiter.acquire();

        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (directUpload) {
                        byte[] content = Files.readAllBytes(localFile.toPath());
                        Blob result = mClient.create(sourceBlob, content);
                        LOG.debug("Upload file {} to gs://{}/{}", localFile, gsBucket, gsKey);
                        LOG.trace("Upload file {}, Blob: {}", result);
                    } else {
                        long startTime = System.nanoTime();
                        try (WriteChannel out = mClient.writer(sourceBlob);
                            FileChannel in = new FileInputStream(localFile).getChannel();
                            ) {
                            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024 * 5); // 5 MiB buffer (remember this is pr. thread)

                            int bytesRead;
                            while ((bytesRead = in.read(buffer)) > 0) {
                                buffer.flip();
                                out.write(buffer);
                                buffer.clear();
                            }
                        }
                        long elapsedTime = System.nanoTime() - startTime;
                        LOG.debug("Upload file {} to gs://{}/{} in {} msec", localFile, gsBucket, gsKey, (elapsedTime / 1000000.0));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }

    private static Storage getService(String credentialsPath, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        if (mStorageService == null) {

            StorageOptions.Builder builder;
            if (credentialsPath != null && !credentialsPath.isEmpty()) {

                try (FileInputStream fis = new FileInputStream(credentialsPath)) {
                    GoogleCredentials credential = GoogleCredentials
                        .fromStream(new FileInputStream(credentialsPath))
                        .createScoped(Collections.singleton(StorageScopes.CLOUD_PLATFORM));

                    // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
                    // Engine), the credentials may require us to specify the scopes we need explicitly.
                    // Check for this case, and inject the scope if required.
                    if (credential.createScopedRequired()) {
                        credential = credential.createScoped(StorageScopes.all());
                    }
                    builder = StorageOptions.newBuilder().setCredentials(credential);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load Google credentials : " + credentialsPath, e);
                }
            } else {
                builder = StorageOptions.getDefaultInstance().toBuilder();
            }

            // These retrySettings are copied from default com.google.cloud.ServiceOptions#getDefaultRetrySettingsBuilder
            // and changed initial retry delay to 5 seconds from 1 seconds.
            // Changed max attempts from 6 to 12.
            builder.setRetrySettings(RetrySettings.newBuilder()
                .setMaxAttempts(12)
                .setInitialRetryDelay(Duration.ofMillis(5000L))
                .setMaxRetryDelay(Duration.ofMillis(32_000L))
                .setRetryDelayMultiplier(2.0)
                .setTotalTimeout(Duration.ofMillis(50_000L))
                .setInitialRpcTimeout(Duration.ofMillis(50_000L))
                .setRpcTimeoutMultiplier(1.0)
                .setMaxRpcTimeout(Duration.ofMillis(50_000L))
                .build());

            mStorageService = builder
                .build()
                .getService();
        }

        return mStorageService;
    }
}
