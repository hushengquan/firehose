package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.message.MessageSerializer;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class LocalFileWriterWrapper {

    private final Constants.WriterType writerType;
    private final int pageSize;
    private final int blockSize;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;
    private final Path basePath;
    @Getter
    private final List<WriterPolicy> policies;
    @Getter
    private final TimePartitionPath timePartitionPath;
    @Getter
    private final MessageSerializer messageSerializer;

    public LocalFileWriter createLocalFileWriter(Path partitionedPath) throws IOException {
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionedPath);
        Path fullPath = dir.resolve(Paths.get(fileName));

        switch (writerType) {
            case PARQUET:
                return new LocalParquetFileWriter(System.currentTimeMillis(), fullPath.toString(), pageSize, blockSize, messageDescriptor, metadataFieldDescriptor);
            default:
                throw new EglcConfigurationException("unsupported file writer type");
        }
    }
}
