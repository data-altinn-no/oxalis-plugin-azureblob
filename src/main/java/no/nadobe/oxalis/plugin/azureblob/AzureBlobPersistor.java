package no.nadobe.oxalis.plugin.azureblob;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.rest.v2.RestException;

import no.difi.oxalis.api.evidence.EvidenceFactory;
import no.difi.oxalis.api.inbound.InboundMetadata;
import no.difi.oxalis.api.lang.EvidenceException;
import no.difi.oxalis.api.lang.OxalisException;
import no.difi.oxalis.api.lang.OxalisSecurityException;
import no.difi.oxalis.api.model.TransmissionIdentifier;
import no.difi.oxalis.api.persist.PayloadPersister;
import no.difi.oxalis.api.persist.ReceiptPersister;
import no.difi.oxalis.api.util.Type;
import no.difi.vefa.peppol.common.model.Header;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.util.FlowableUtil;

import io.reactivex.*;
import io.reactivex.Flowable;

@Singleton
public class AzureBlobPersistor implements PayloadPersister, ReceiptPersister {
    public static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobPersistor.class);

    private static final String CONTAINERNAME = "oxalisinbound";
    private static final Integer BLOCKSIZE = 8 * 1024 * 1024;

    private final Path inboundFolder;
    private final EvidenceFactory evidenceFactory;
    private final String accountName;
    private final String accountKey;
    private ContainerURL containerURL;

    // public AzureBlobPersistor(@Named("accountname") String accountName,
    // @Named("accountkey") String accountKey, EvidenceFactory evidenceFactory) {
    @Inject
    public AzureBlobPersistor(EvidenceFactory evidenceFactory) throws OxalisException {
        // FIXME! This should be settings
        this.accountName = "oxalisinbound01";
        this.accountKey = "CHANGEME";
        this.evidenceFactory = evidenceFactory;
        this.inboundFolder = new File("/tmp").toPath();

        Initialize();

        LOGGER.debug("Initialized {}, account name: {}", AzureBlobPersistor.class.getName(), this.accountName);
    }

    public void persist(InboundMetadata inboundMetadata, Path payloadPath) throws IOException {

        LOGGER.debug("Receipt received with id {}",
                FilterString(inboundMetadata.getTransmissionIdentifier().getIdentifier()));

        String blobIdentifier = GetBlobIdentifier(inboundMetadata.getTransmissionIdentifier(),
                inboundMetadata.getHeader(), "receipt.dat");
        Path outputPath = inboundFolder.resolve(Paths.get(blobIdentifier));

        try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
            evidenceFactory.write(outputStream, inboundMetadata);
        } catch (EvidenceException e) {
            throw new IOException("Unable to persist receipt.", e);
        }

        LOGGER.debug("Receipt temporarily persisted to: {}", outputPath);

        final BlockBlobURL blobURL = containerURL.createBlockBlobURL(blobIdentifier);
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(outputPath);

        TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, BLOCKSIZE, null).subscribe(response -> {
            if (response.response().statusCode() == 201) {
                LOGGER.info("Uploaded file to {}, deleting temporary local file", blobURL.toString());
                Files.delete(outputPath);
            } else {
                LOGGER.warn("Failed to upload temporary file {} to blob URL {}, status code was {}",
                        outputPath.toString(), blobURL.toString(), response.response().statusCode());
            }
        });
    }

    public Path persist(TransmissionIdentifier transmissionIdentifier, Header header, InputStream inputStream)
            throws IOException {

        LOGGER.debug("Payload received with id {}", FilterString(transmissionIdentifier.getIdentifier()));

        String blobIdentifier = GetBlobIdentifier(transmissionIdentifier, header, "payload.xml");
        Path outputPath = inboundFolder.resolve(Paths.get(blobIdentifier));

        try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
            ByteStreams.copy(inputStream, outputStream);
        }

        LOGGER.info("Payload temporarily persisted to: {}", outputPath);

        final BlockBlobURL blobURL = containerURL.createBlockBlobURL(blobIdentifier);
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(outputPath);

        TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, BLOCKSIZE, null).subscribe(response -> {
            if (response.response().statusCode() == 201) {
                LOGGER.info("Uploaded file to {}, deleting temporary local file", blobURL.toString());
                Files.delete(outputPath);
            } else {
                LOGGER.warn("Failed to upload temporary file {} to blob URL {}, status code was {}",
                        outputPath.toString(), blobURL.toString(), response.response().statusCode());
            }
        });

        return outputPath;
    }

    private void Initialize() throws OxalisException {
        containerURL = GetContainerURL(GetServiceURL(accountName, GetSharedKeyCredentials(accountName, accountKey)),
                CONTAINERNAME);
    }

    private SharedKeyCredentials GetSharedKeyCredentials(String accountName, String accountKey)
            throws OxalisSecurityException {
        try {
            return new SharedKeyCredentials(accountName, accountKey);
        } catch (InvalidKeyException e) {
            throw new OxalisSecurityException("Invalid account key", e);
        }
    }

    private ServiceURL GetServiceURL(String accountName, SharedKeyCredentials creds) throws OxalisSecurityException {
        try {
            return new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"),
                    StorageURL.createPipeline(creds, new PipelineOptions()));
        } catch (MalformedURLException e) {
            throw new OxalisSecurityException("Invalid account name", e);
        }
    }

    private ContainerURL GetContainerURL(ServiceURL serviceURL, String containerName) throws EvidenceException {

        ContainerURL containerURL = serviceURL.createContainerURL(containerName);

        LOGGER.debug("Attempting to create container '{}'", containerName);

        try {
            ContainerCreateResponse response = containerURL.create(null, null, null).blockingGet();
            LOGGER.info("Created container '{}'", containerName);

        } catch (RestException e) {
            if (e instanceof RestException && ((RestException) e).response().statusCode() != 409) {
                throw new EvidenceException("Failed to create container '" + containerName + "'", e);
            } else {
                LOGGER.debug("The container '{}' already exists, continuing", containerName);
            }
        }

        return containerURL;
    }

    private String FilterString(String s) {
        return s.replaceAll("[^a-zA-Z0-9.\\-]", "_");
    }

    private String GetBlobIdentifier(TransmissionIdentifier transmissionIdentifier, Header header, String suffix) {
        return String.format("%s_%s_%s.%s", FilterString(header.getReceiver().getIdentifier()),
                FilterString(header.getSender().getIdentifier()), FilterString(transmissionIdentifier.getIdentifier()),
                suffix);
    }
}
