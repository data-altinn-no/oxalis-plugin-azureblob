package no.nadobe.oxalis.plugin.azureblob;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.rest.v2.RestException;

import no.difi.oxalis.api.evidence.EvidenceFactory;
import no.difi.oxalis.api.inbound.InboundMetadata;
import no.difi.oxalis.api.lang.EvidenceException;
import no.difi.oxalis.api.lang.OxalisException;
import no.difi.oxalis.api.lang.OxalisSecurityException;
import no.difi.oxalis.api.model.TransmissionIdentifier;
import no.difi.oxalis.api.persist.PayloadPersister;
import no.difi.oxalis.api.persist.ReceiptPersister;
import no.difi.vefa.peppol.common.model.Header;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.TransferManager;

@Singleton
public class AzureBlobPersistor implements PayloadPersister, ReceiptPersister {
    public static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobPersistor.class);

    private final EvidenceFactory evidenceFactory;
    private Path tempInboundFolder;
    private SharedKeyCredentials sharedKeyCredentials;

    private String containerName = "inbound";
    private Integer blockSize = 1024 * 1024 * 8;

    @Inject
    public AzureBlobPersistor(EvidenceFactory evidenceFactory) throws OxalisException, IllegalArgumentException {

	this.evidenceFactory = evidenceFactory;
	Initialize();
	LOGGER.debug("Initialized {}, account name: {}", AzureBlobPersistor.class.getName());
    }

    public void persist(InboundMetadata inboundMetadata, Path payloadPath) throws IOException {

	LOGGER.debug("Receipt received with id {}",
		FilterString(inboundMetadata.getTransmissionIdentifier().getIdentifier()));

	String blobIdentifier = GetBlobIdentifier(inboundMetadata.getTransmissionIdentifier(),
		inboundMetadata.getHeader(), "receipt.dat");
	Path outputPath = tempInboundFolder.resolve(Paths.get(blobIdentifier));

	try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
	    evidenceFactory.write(outputStream, inboundMetadata);
	} catch (EvidenceException e) {
	    throw new IOException("Unable to persist receipt.", e);
	}

	LOGGER.debug("Receipt temporarily persisted to: {}", outputPath);

	final BlockBlobURL blobURL = GetContainerURL()
		.createBlockBlobURL(GetDateSeperatedBlobIdentifier("receipt", blobIdentifier));
	AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(outputPath);

	TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, blockSize, null).subscribe(response -> {
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
	Path outputPath = tempInboundFolder.resolve(Paths.get(blobIdentifier));

	try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
	    ByteStreams.copy(inputStream, outputStream);
	}

	LOGGER.info("Payload temporarily persisted to: {}", outputPath);

	final BlockBlobURL blobURL = GetContainerURL()
		.createBlockBlobURL(GetDateSeperatedBlobIdentifier("payload", blobIdentifier));
	AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(outputPath);

	TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, blockSize, null).subscribe(response -> {
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

    private void Initialize() throws OxalisException, IllegalArgumentException {

	String connectionString = System.getenv("AZURE_STORAGE_ACCOUNT_CONNECTION_STRING");

	if (connectionString == null || connectionString == "") {
	    LOGGER.error("Environment variable AZURE_STORAGE_ACCOUNT_CONNECTION_STRING not set");
	    throw new IllegalArgumentException("Environment variable AZURE_STORAGE_ACCOUNT_CONNECTION_STRING not set");
	}

	this.tempInboundFolder = new File("/tmp").toPath();
	this.sharedKeyCredentials = GetSharedKeyCredentials(connectionString);
    }

    private SharedKeyCredentials GetSharedKeyCredentials(String connectionString) throws OxalisSecurityException {
	try {
	    String[] s = ParseAccountNameAndKeyFromConnectionString(connectionString);
	    return new SharedKeyCredentials(s[0], s[1]);
	} catch (IllegalArgumentException e) {
	    throw new OxalisSecurityException("Unable to parse connection string", e);
	} catch (InvalidKeyException e) {
	    throw new OxalisSecurityException("Invalid account key", e);
	}
    }

    private ServiceURL GetServiceURL(SharedKeyCredentials creds) throws IOException {
	try {
	    return new ServiceURL(new URL("https://" + creds.getAccountName() + ".blob.core.windows.net"),
		    StorageURL.createPipeline(creds, new PipelineOptions()));
	} catch (MalformedURLException e) {
	    throw new IOException("Invalid account name", e);
	}
    }

    private ContainerURL GetContainerURL() throws IOException {

	ServiceURL serviceURL = GetServiceURL(this.sharedKeyCredentials);
	ContainerURL containerURL = serviceURL.createContainerURL(containerName);

	LOGGER.debug("Attempting to create container '{}'", containerName);

	try {
	    containerURL.create(null, null, null).blockingGet();
	    LOGGER.info("Created container '{}'", containerName);

	} catch (RestException e) {
	    if (e instanceof RestException && ((RestException) e).response().statusCode() != 409) {
		throw new IOException("Failed to create container '" + containerName + "'", e);
	    } else {
		LOGGER.debug("The container '{}' already exists, continuing", containerName);
	    }
	}

	return containerURL;
    }

    private String[] ParseAccountNameAndKeyFromConnectionString(String connectionString)
	    throws IllegalArgumentException {
	Matcher an = Pattern.compile("AccountName=([^;]+);", Pattern.CASE_INSENSITIVE).matcher(connectionString);
	Matcher ak = Pattern.compile("AccountKey=([^;]+);", Pattern.CASE_INSENSITIVE).matcher(connectionString);

	if (an.find() && ak.find() && an.groupCount() == 1 && ak.groupCount() == 1) {
	    return new String[] { an.group(1), ak.group(1) };
	} else {
	    throw new IllegalArgumentException("Unable to parse connection string");
	}
    }

    private String FilterString(String s) {
	return s.replaceAll("[^a-zA-Z0-9.\\-]", "_");
    }

    private String GetBlobIdentifier(TransmissionIdentifier transmissionIdentifier, Header header, String suffix) {
	return String.format("%s_%s_%s.%s", FilterString(header.getReceiver().getIdentifier()),
		FilterString(header.getSender().getIdentifier()), FilterString(transmissionIdentifier.getIdentifier()),
		suffix);
    }

    private String GetDateSeperatedBlobIdentifier(String prefix, String blobIdentifier) {
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
	return prefix + "/" + dateFormat.format(new Date()) + "/" + blobIdentifier;
    }
}
