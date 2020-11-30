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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.talend.components.azure.dlsgen2.blob.helpers.FileMaskTable;
import org.talend.components.azure.dlsgen2.blob.tazurestorageput.TAzureDlsGen2PutProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

/**
 * Upload a set of files form a local folder to Azure blob storage
 */
public class AzureDlsGen2PutRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = -9091715000681485918L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2PutRuntime.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2PutRuntime.class);

    private String localFolder;

    private String remoteFolder;

    private Boolean useFileList;

    private FileMaskTable files;

    public AzureDlsGen2BlobService azureDlsGen2BlobService;

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }

        TAzureDlsGen2PutProperties componentProperties = (TAzureDlsGen2PutProperties) properties;
        localFolder = componentProperties.localFolder.getValue();
        remoteFolder = componentProperties.remoteFolder.getValue();
        useFileList = componentProperties.useFileList.getValue();
        files = componentProperties.files;
        this.dieOnError = componentProperties.dieOnError.getValue();
        this.azureDlsGen2BlobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        // checks local folder
        String errorMessage = "";
        if (!new File(localFolder).exists()) {
            errorMessage = messages.getMessage("error.EmptyLocalFolder"); //$NON-NLS-1$
        }
        // checks file list if set.
        else if (useFileList && files.fileMask.getValue().isEmpty()) {
            errorMessage = messages.getMessage("error.EmptyFileList"); //$NON-NLS-1$
        }

        if (errorMessage.isEmpty()) { // everything is OK.
            return ValidationResult.OK;
        } else {
            return new ValidationResult(ValidationResult.Result.ERROR, errorMessage);
        }
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {
        upload(runtimeContainer);
        setReturnValues(runtimeContainer);
    }

    private void upload(RuntimeContainer runtimeContainer) {

        AzureDlsGen2Utils utils = new AzureDlsGen2Utils();
        List<Map<String, String>> list = new ArrayList<>();
        // process files list
        if (useFileList && files != null && files.size() > 0) {
            for (int idx = 0; idx < files.fileMask.getValue().size(); idx++) {
                String fileMask = files.fileMask.getValue().get(idx);
                String newName = files.newName.getValue().get(idx);
                Map<String, String> map = new HashMap<>();
                map.put(fileMask, newName);
                list.add(map);
            }
        }
        Map<String, String> fileMap;
        if (useFileList) {
            fileMap = utils.genFileFilterList(list, localFolder, remoteFolder);
        } else {
            fileMap = utils.genAzureObjectList(new File(localFolder), remoteFolder);
        }
        for (Map.Entry<String, String> entry : fileMap.entrySet()) {
            File source = new File(entry.getKey());
            try (FileInputStream stream = new FileInputStream(source)) { // see try-with-resources concept

                // TODO Any Action ??? if remoteFolder doesn't exist it will fail...
                azureDlsGen2BlobService.upload(containerName, entry.getValue(), stream, source.length());

            } catch (BlobStorageException | IOException e) {
                if (BlobStorageException.class.isInstance(e)) {
                    LOGGER.error("[put] status: {} message: {}.", BlobStorageException.class.cast(e)
                            .getStatusCode(), BlobStorageException.class.cast(e).getMessage());
                } else {
                    LOGGER.error(e.getLocalizedMessage());
                }
                if (dieOnError) {
                    throw new ComponentException(e);
                }
            }
        }
    }

    public void setReturnValues(RuntimeContainer runtimeContainer) {

        String componentId = runtimeContainer.getCurrentComponentId();
        String containerKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER);
        String localFolderKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2BlobDefinition.RETURN_LOCAL_FOLDER);
        String remoteFolderKey = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2BlobDefinition.RETURN_REMOTE_FOLDER);

        runtimeContainer.setComponentData(componentId, containerKey, this.containerName);
        runtimeContainer.setComponentData(componentId, localFolderKey, this.localFolder);
        runtimeContainer.setComponentData(componentId, remoteFolderKey, this.remoteFolder);

    }

}
