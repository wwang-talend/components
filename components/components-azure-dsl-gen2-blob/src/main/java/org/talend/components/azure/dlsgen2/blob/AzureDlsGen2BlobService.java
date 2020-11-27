// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.azure.dlsgen2.blob;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.PublicAccessType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.azure.dlsgen2.AzureDlsGen2Connection;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;

/**
 * This class encapsulate and provide azure storage blob services
 */
public class AzureDlsGen2BlobService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2BlobService.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2BlobService.class);

    public static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(80);

    private AzureDlsGen2Connection connection;

    /**
     * @param connection
     */
    public AzureDlsGen2BlobService(final AzureDlsGen2Connection connection) {
        super();
        this.connection = connection;
    }

    private BlobContainerClient getContainerReference(String container) {
        return connection.getBlobServiceClient().getBlobContainerClient(container);
    }

    /**
     * This method create an azure container if it doesn't exist and set it access policy
     *
     * @param containerName : the name of the container to be created
     *
     * @return true if the container was created, false otherwise
     */
    public boolean createContainerIfNotExist(final String containerName, final PublicAccessType accessType)
            throws BlobStorageException {
        final BlobContainerClient container = getContainerReference(containerName);

        boolean containerCreated;
        try {
            container.createWithResponse(Collections.emptyMap(), accessType, OPERATION_TIMEOUT, AzureDlsGen2Utils
                    .getTalendOperationContext());
            containerCreated = true;
        } catch (BlobStorageException e) {
            if (!e.getErrorCode().equals(BlobErrorCode.CONTAINER_BEING_DELETED)) {
                throw e;
            }
            LOGGER.warn(messages.getMessage("error.CONTAINER_BEING_DELETED", containerName));
            // wait 40 seconds (min is 30s) before retrying.
            // See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/delete-container
            try {
                Thread.sleep(OPERATION_TIMEOUT.toMillis());
            } catch (InterruptedException eint) {
                LOGGER.error(messages.getMessage("error.InterruptedException"));
                throw new ComponentException(eint);
            }
            try {
                container.createWithResponse(Collections.emptyMap(), accessType, OPERATION_TIMEOUT, AzureDlsGen2Utils
                        .getTalendOperationContext());
                containerCreated = true;
                LOGGER.debug(messages.getMessage("debug.ContainerCreated", containerName));
            } catch (BlobStorageException exception) {
                throw (exception);
            }
        }

        return containerCreated;
    }

    /**
     * This method delete the container if exist
     */
    public boolean deleteContainerIfExist(final String containerName) throws BlobStorageException {
        getContainerReference(containerName)
                .deleteWithResponse(null, OPERATION_TIMEOUT, AzureDlsGen2Utils.getTalendOperationContext());
        return true;
    }

    /**
     * @return true if the a container exist with the given name, false otherwise
     */
    public boolean containerExist(final String containerName) throws BlobStorageException {
        return getContainerReference(containerName)
                .existsWithResponse(OPERATION_TIMEOUT, AzureDlsGen2Utils.getTalendOperationContext())
                .getValue();
    }

    public PagedIterable<BlobContainerItem> listContainers() {
        return connection.getBlobServiceClient().listBlobContainers();
    }

    public PagedIterable<BlobItem> listBlobs(final String containerName, final String prefix, final boolean useFlatBlobListing)
            throws BlobStorageException {
        ListBlobsOptions options = new ListBlobsOptions()
                .setPrefix(prefix)
                .setDetails(new BlobListDetails().setRetrieveMetadata(true));
        return getContainerReference(containerName).listBlobs(options, OPERATION_TIMEOUT);
    }

    public void deleteBlob(final String containerName, final String blobName) throws BlobStorageException {
        LOGGER.debug("[deleteBlob] Downloading blob {} from container {}.", blobName, containerName);
        getContainerReference(containerName).getBlobClient(blobName).delete();
    }

    public void download(final String containerName, final String blobName, final OutputStream outStream) throws BlobStorageException {
        LOGGER.debug("[download] Downloading blob {} from container {}.", blobName, containerName);
        getContainerReference(containerName).getBlobClient(blobName).download(outStream);
    }

    public void upload(final String containerName, final String blobName, final InputStream sourceStream, final long length) throws BlobStorageException {
        LOGGER.debug("[upload] Uploading blob {} to container {}.", blobName, containerName);
        getContainerReference(containerName).getBlobClient(blobName).upload(sourceStream, length);
    }

}
