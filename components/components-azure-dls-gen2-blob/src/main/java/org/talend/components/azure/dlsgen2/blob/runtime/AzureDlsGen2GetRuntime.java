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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobDefinition;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlobGet;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlobsGetTable;
import org.talend.components.azure.dlsgen2.blob.tazurestorageget.TAzureDlsGen2GetProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2GetRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = 3439883025371560977L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2GetRuntime.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2GetRuntime.class);

    private String localFolder;

    private boolean keepRemoteDirStructure;

    private RemoteBlobsGetTable remoteBlobsGet;

    /**
     * keep this attribute public for test purpose
     */
    public AzureDlsGen2BlobService azureDlsGen2BlobService;

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }

        TAzureDlsGen2GetProperties componentProperties = (TAzureDlsGen2GetProperties) properties;
        localFolder = componentProperties.localFolder.getValue();
        keepRemoteDirStructure = componentProperties.keepRemoteDirStructure.getValue();
        remoteBlobsGet = componentProperties.remoteBlobsGet;
        this.dieOnError = componentProperties.dieOnError.getValue();
        azureDlsGen2BlobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        String errorMessage = "";
        if (remoteBlobsGet.prefix.getValue() == null || remoteBlobsGet.prefix.getValue().isEmpty()) {

            errorMessage = messages.getMessage("error.EmptyBlobs"); //$NON-NLS-1$
        }

        if (errorMessage.isEmpty()) { // everything is OK.
            return ValidationResult.OK;
        } else {
            return new ValidationResult(ValidationResult.Result.ERROR, errorMessage);
        }
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {
        download(runtimeContainer);
        setReturnValues(runtimeContainer);

    }

    private void download(RuntimeContainer runtimeContainer) {
        FileOutputStream fos = null;

        try {
            List<RemoteBlobGet> remoteBlobs = createRemoteBlobsGet();
            for (RemoteBlobGet rmtb : remoteBlobs) {
                for (BlobItem blob : azureDlsGen2BlobService.listBlobs(containerName, rmtb.prefix, rmtb.include)) {
                    if (isBlobSelectable(rmtb.include, rmtb.prefix, blob.getName()) && !isDirectoryBlob(blob)) {
                        // TODO - Action when create is false and include is true ???
                        if (keepRemoteDirStructure) {
                            if (rmtb.create) {
                                new File(localFolder + "/" + blob.getName()).getParentFile().mkdirs();
                            }
                            fos = new FileOutputStream(localFolder + "/" + blob.getName());
                        } else {
                            String blobFullName = blob.getName();
                            String resultFileName = blobFullName;
                            String prefixDir = rmtb.prefix.contains("/") ? rmtb.prefix
                                    .substring(0, rmtb.prefix.lastIndexOf("/")) : rmtb.prefix;
                            if (blobFullName.startsWith(prefixDir + "/")) {
                                resultFileName = blobFullName.substring(prefixDir.length());
                            }

                            File pathToWrite = new File(localFolder + "/" + resultFileName);
                            if (rmtb.create) {
                                pathToWrite.getParentFile().mkdirs();
                            }

                            fos = new FileOutputStream(pathToWrite);
                        }
                        azureDlsGen2BlobService.download(containerName, blob.getName(), fos);

                    }
                }
            }
        } catch (BlobStorageException | FileNotFoundException e) {
            if (BlobStorageException.class.isInstance(e)) {
                LOGGER.error("[download] status: {} message: {}.", BlobStorageException.class.cast(e)
                        .getStatusCode(), BlobStorageException.class.cast(e).getMessage());
            } else {
                LOGGER.error(e.getLocalizedMessage());
            }
            if (dieOnError) {
                throw new ComponentException(e);
            }
        } finally {
            try {
                fos.close();
            } catch (Exception e) {
                // ignore
            }
        }

    }

    public List<RemoteBlobGet> createRemoteBlobsGet() {

        List<RemoteBlobGet> remoteBlobs = new ArrayList<>();
        for (int idx = 0; idx < remoteBlobsGet.prefix.getValue().size(); idx++) {
            String prefix = "";
            boolean include = false;
            boolean create = false;

            if (remoteBlobsGet.prefix.getValue().get(idx) != null) {
                prefix = remoteBlobsGet.prefix.getValue().get(idx);
            }

            if (remoteBlobsGet.include.getValue().get(idx) != null) {
                include = remoteBlobsGet.include.getValue().get(idx);
            }

            if (remoteBlobsGet.create.getValue().get(idx) != null) {
                create = remoteBlobsGet.create.getValue().get(idx);
            }

            remoteBlobs.add(new RemoteBlobGet(prefix, include, create));
        }
        return remoteBlobs;
    }

    public void setReturnValues(RuntimeContainer runtimeContainer) {

        String componentId = runtimeContainer.getCurrentComponentId();
        String containerKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER);
        String localFolderKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2BlobDefinition.RETURN_LOCAL_FOLDER);

        runtimeContainer.setComponentData(componentId, containerKey, this.containerName);
        runtimeContainer.setComponentData(componentId, localFolderKey, this.localFolder);
    }

}
