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


import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerdelete.TAzureDlsGen2ContainerDeleteProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2ContainerDeleteRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = -4320180294438040454L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ContainerDeleteRuntime.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2ContainerDeleteRuntime.class);

    /**
     * let this attribute public for test purpose
     */
    public AzureDlsGen2BlobService blobService;

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }
        TAzureDlsGen2ContainerDeleteProperties componentProperties = (TAzureDlsGen2ContainerDeleteProperties) properties;
        this.dieOnError = componentProperties.dieOnError.getValue();
        this.blobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        return ValidationResult.OK;
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {

        deleteBlobContainerIfExists(runtimeContainer);
        setReturnValues(runtimeContainer);
    }

    private void deleteBlobContainerIfExists(RuntimeContainer runtimeContainer) {
        try {
            blobService.deleteContainerIfExist(this.containerName);
        } catch (BlobStorageException e) {
            if (e.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
                LOGGER.warn(messages.getMessage("error.ContainerNotExist", this.containerName));
            } else {
                LOGGER.error(e.getLocalizedMessage());
            }
            if (this.dieOnError) {
                throw new ComponentException(e);
            }
        }
    }

    private void setReturnValues(RuntimeContainer runtimeContainer) {
        String componentId = runtimeContainer.getCurrentComponentId();
        String returnContainer = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER);

        runtimeContainer.setComponentData(componentId, returnContainer, containerName);
    }

}
