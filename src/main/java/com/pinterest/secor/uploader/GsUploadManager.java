package com.pinterest.secor.uploader;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
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
 *
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

        mClient = getService(mConfig.getGsCredentialsPath());
    }

    @Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        final String gsBucket = mConfig.getGsBucket();
        final String gsKey = localPath.withPrefix(mConfig.getGsPath()).getLogFilePath();
        File localFile = new File(localPath.getLogFilePath());

        LOG.info("uploading file {} to gs://{}/{}", localFile, gsBucket, gsKey);

        final StorageObject storageObject = new StorageObject().setName(gsKey);
        final FileContent storageContent = new FileContent(Files.probeContentType(localFile.toPath()), localFile);

        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    mClient.objects().insert(gsBucket, storageObject, storageContent).execute();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }

    private static Storage getService(String credentialsPath) throws Exception {
        if (mStorageService == null) {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

            GoogleCredential credential;
            try {
                // Lookup if configured path from the properties; otherwise fallback to Google Application default
                if (credentialsPath != null) {
                    credential = GoogleCredential.fromStream(new FileInputStream(credentialsPath), httpTransport, JSON_FACTORY)
                            .createScoped(Collections.singleton(StorageScopes.CLOUD_PLATFORM));
                } else {
                    credential = GoogleCredential.getApplicationDefault(httpTransport, JSON_FACTORY);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load Google credentials : " + credentialsPath, e);
            }

            mStorageService = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
                    .setApplicationName("com.pinterest.secor")
                    .build();
        }
        return mStorageService;
    }

}
