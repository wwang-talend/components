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

import com.azure.storage.blob.models.BlobStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist.TAzureDlsGen2ContainerExistDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist.TAzureDlsGen2ContainerExistProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2ContainerExistRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = 8454949161040534258L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ContainerExistRuntime.class);

    /**
     * let this attribute public for test purpose
     */
    public AzureDlsGen2BlobService azureDlsGen2BlobService;

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }
        TAzureDlsGen2ContainerExistProperties componentProperties = (TAzureDlsGen2ContainerExistProperties) properties;
        this.dieOnError = componentProperties.dieOnError.getValue();
        this.azureDlsGen2BlobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        return ValidationResult.OK;
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {
        boolean containerExist = isAzureStorageBlobContainerExist(runtimeContainer);
        setReturnValues(runtimeContainer, containerExist);
    }

    private Boolean isAzureStorageBlobContainerExist(RuntimeContainer runtimeContainer) {
        try {

            return azureDlsGen2BlobService.containerExist(this.containerName);

        } catch (BlobStorageException e) {
            LOGGER.error(e.getLocalizedMessage());
            if (this.dieOnError) {
                throw new ComponentException(e);
            }
            return false;
        }
    }

    private void setReturnValues(RuntimeContainer runtimeContainer, boolean containerExist) {
        String componentId = runtimeContainer.getCurrentComponentId();
        String returnContainer = AzureDlsGen2Utils
                .getStudioNameFromProperty(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER);
        String returnContainerExist = AzureDlsGen2Utils
                .getStudioNameFromProperty(TAzureDlsGen2ContainerExistDefinition.RETURN_CONTAINER_EXIST);

        runtimeContainer.setComponentData(componentId, returnContainer, containerName);
        runtimeContainer.setComponentData(componentId, returnContainerExist, containerExist);
    }

}
