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
package org.talend.components.azure.dlsgen2.blob.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlob;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlobsTable;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2DeleteRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = -1061435894570205595L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2DeleteRuntime.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2DeleteRuntime.class);

    private RemoteBlobsTable remoteBlobsTable;

    public AzureDlsGen2BlobService azureDlsGen2BlobService;

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }

        TAzureDlsGen2DeleteProperties componentProperties = (TAzureDlsGen2DeleteProperties) properties;
        remoteBlobsTable = componentProperties.remoteBlobs;
        this.dieOnError = componentProperties.dieOnError.getValue();
        this.azureDlsGen2BlobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        return componentProperties.remoteBlobs.getValidationResult();
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {
        delete(runtimeContainer);
        setReturnValues(runtimeContainer);
    }

    /**
     * Delete a blob item
     *
     * Notes from API docs:
     * If you use the Delete Blob API to delete a directory, that directory will be deleted only if it's empty.
     * This means that you can't use the Blob API delete directories recursively.
     *
     * @param runtimeContainer
     */
    public void delete(RuntimeContainer runtimeContainer) {
        List<String> removeFoldersAfter = new ArrayList<>();
        try {
            for (RemoteBlob rmtb : createRemoteBlobFilter()) {
                for (BlobItem blob : azureDlsGen2BlobService.listBlobs(containerName, rmtb.prefix, rmtb.include)) {
                    if (isBlobSelectable(rmtb.include, rmtb.prefix, blob.getName())) {
                        try {
                            azureDlsGen2BlobService.deleteBlob(containerName, blob.getName());
                        } catch (BlobStorageException e) {
                            if (e.getStatusCode() == 409) {
                                if (rmtb.include) {
                                    removeFoldersAfter.add(blob.getName());
                                }
                            } else {
                                LOGGER.error("{} {}.", e.getStatusCode(), e.getMessage());
                                if (dieOnError) {
                                    throw new ComponentException(e);
                                }
                            }
                        }
                    }
                }
            }
            Comparator<String> comparator = (s1, s2) -> Integer.compare(s1.split("/").length, s2.split("/").length);
            removeFoldersAfter.stream()
                    .sorted(comparator.reversed())
                    .forEach(folder -> azureDlsGen2BlobService.deleteBlob(containerName, folder));
        } catch (BlobStorageException e) {
            LOGGER.error(e.getLocalizedMessage());
            if (dieOnError) {
                throw new ComponentException(e);
            }
        }
    }

    /**
     * Create remote blob table used in filtering remote blob
     */
    private List<RemoteBlob> createRemoteBlobFilter() {
        List<RemoteBlob> remoteBlobs = new ArrayList<RemoteBlob>();
        for (int idx = 0; idx < this.remoteBlobsTable.prefix.getValue().size(); idx++) {
            String prefix = "";
            boolean include = false;
            if (this.remoteBlobsTable.prefix.getValue().get(idx) != null) {
                prefix = this.remoteBlobsTable.prefix.getValue().get(idx);
            }
            if (remoteBlobsTable.include.getValue().get(idx) != null) {
                include = remoteBlobsTable.include.getValue().get(idx);
            }

            remoteBlobs.add(new RemoteBlob(prefix, include));
        }

        return remoteBlobs;
    }

    public void setReturnValues(RuntimeContainer runtimeContainer) {

        String componentId = runtimeContainer.getCurrentComponentId();
        String containerKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER);

        runtimeContainer.setComponentData(componentId, containerKey, this.containerName);
    }

}
