package com.pinterest.secor.uploader;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    private static final ExecutorService executor = Executors.newFixedThreadPool(256);

    /**
     * Global instance of the Storage. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static Storage mStorageService;

    private Storage mClient;

    public GsUploadManager(SecorConfig config) throws Exception {
        super(config);

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

        final StorageObject storageObject = new StorageObject().setName(gsKey);
        final FileContent storageContent = new FileContent(Files.probeContentType(localFile.toPath()), localFile);

        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Storage.Objects.Insert request = mClient.objects().insert(gsBucket, storageObject, storageContent);

                    if (directUpload) {
                        request.getMediaHttpUploader().setDirectUploadEnabled(true);
                    }

                    request.getMediaHttpUploader().setProgressListener(new MediaHttpUploaderProgressListener() {
                        @Override
                        public void progressChanged(MediaHttpUploader uploader) throws IOException {
                            LOG.debug("[{} %] upload file {} to gs://{}/{}",
                                    (int) uploader.getProgress() * 100, localFile, gsBucket, gsKey);
                        }
                    });

                    request.execute();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }

    private static Storage getService(String credentialsPath, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        if (mStorageService == null) {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

            GoogleCredential credential;
            try {
                // Lookup if configured path from the properties; otherwise fallback to Google Application default
                if (credentialsPath != null && !credentialsPath.isEmpty()) {
                    credential = GoogleCredential
                            .fromStream(new FileInputStream(credentialsPath), httpTransport, JSON_FACTORY)
                            .createScoped(Collections.singleton(StorageScopes.CLOUD_PLATFORM));
                } else {
                    credential = GoogleCredential.getApplicationDefault(httpTransport, JSON_FACTORY);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load Google credentials : " + credentialsPath, e);
            }

            // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
            // Engine), the credentials may require us to specify the scopes we need explicitly.
            // Check for this case, and inject the scope if required.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(StorageScopes.all());
            }

            mStorageService = new Storage.Builder(httpTransport, JSON_FACTORY,
                    setHttpBackoffTimeout(credential, connectTimeoutMs, readTimeoutMs))
                    .setApplicationName("com.pinterest.secor")
                    .build();
        }
        return mStorageService;
    }

    private static HttpRequestInitializer setHttpBackoffTimeout(final HttpRequestInitializer requestInitializer,
                                                                final int connectTimeoutMs, final int readTimeoutMs) {
        return new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest httpRequest) throws IOException {
                requestInitializer.initialize(httpRequest);

                // Configure exponential backoff on error
                // https://developers.google.com/api-client-library/java/google-http-java-client/backoff
                ExponentialBackOff backoff = new ExponentialBackOff();
                HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(backoff)
                        .setBackOffRequired(HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ALWAYS);
                httpRequest.setUnsuccessfulResponseHandler(backoffHandler);

                httpRequest.setConnectTimeout(connectTimeoutMs);
                httpRequest.setReadTimeout(readTimeoutMs);
            }
        };
    }

}
