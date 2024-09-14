package com.gotocompany.firehose.sink.common.blobstorage;

import com.gotocompany.firehose.config.COSConfig;
import com.gotocompany.firehose.config.GCSConfig;
import com.gotocompany.firehose.config.S3Config;
import com.gotocompany.firehose.sink.common.blobstorage.cos.TencentCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;

public class BlobStorageFactory {

    public static BlobStorage createObjectStorage(BlobStorageType storageType, Map<String, String> config) {
        switch (storageType) {
            case GCS:
                try {
                    GCSConfig gcsConfig = ConfigFactory.create(GCSConfig.class, config);
                    return new GoogleCloudStorage(gcsConfig);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Exception while creating GCS Storage", e);
                }
            case S3:
                try {
                    S3Config s3Config = ConfigFactory.create(S3Config.class, config);
                    return new S3(s3Config);
                 } catch (Exception e) {
                    throw new IllegalArgumentException("Exception while creating S3 Storage", e);
                }
            case COS:
                try {
                    COSConfig cosConfig = ConfigFactory.create(COSConfig.class, config);
                    return new TencentCloudStorage(cosConfig);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Exception while creating COS Storage", e);
                }

            default:
                throw new IllegalArgumentException("Blob Storage Type " + storageType + " is not supported");
        }
    }
}
