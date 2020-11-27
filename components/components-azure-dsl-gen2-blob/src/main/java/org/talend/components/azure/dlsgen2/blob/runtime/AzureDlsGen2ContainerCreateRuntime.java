package org.talend.components.azure.dlsgen2.blob.runtime;

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.PublicAccessType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateProperties;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateProperties.AccessControl;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

/**
 * Runtime implementation for Azure storage container create feature.<br/>
 * These methods are called only on Driver node in following order: <br/>
 * 1) {@link this#initialize(RuntimeContainer, ComponentProperties)} <br/>
 * 2) {@link this#runAtDriver(RuntimeContainer)} <br/>
 * <b>Instances of this class should not be serialized and sent on worker nodes</b>
 */
public class AzureDlsGen2ContainerCreateRuntime extends AzureDlsGen2ContainerRuntime
        implements ComponentDriverInitialization<ComponentProperties> {

    private static final long serialVersionUID = -8413348199906078372L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ContainerCreateRuntime.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2ContainerCreateRuntime.class);

    private AccessControl access;

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

        TAzureDlsGen2ContainerCreateProperties componentProperties = (TAzureDlsGen2ContainerCreateProperties) properties;
        this.access = componentProperties.accessControl.getValue();
        this.dieOnError = componentProperties.dieOnError.getValue();
        this.blobService = new AzureDlsGen2BlobService(getAzureConnection(runtimeContainer));

        return ValidationResult.OK;
    }

    @Override
    public void runAtDriver(RuntimeContainer runtimeContainer) {

        createAzureStorageBlobContainer();
        setReturnValues(runtimeContainer);
    }

    private void createAzureStorageBlobContainer() {
        try {
            PublicAccessType accessType = PublicAccessType.BLOB;
            accessType = AccessControl.Public.equals(access) ? PublicAccessType.BLOB : PublicAccessType.CONTAINER;
            boolean containerCreated = blobService.createContainerIfNotExist(containerName, accessType);
            if (!containerCreated) {
                LOGGER.warn(messages.getMessage("warn.ContainerExists", containerName));
            }
        } catch (BlobStorageException e) {
            LOGGER.error(e.getLocalizedMessage());
            if (dieOnError) {
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
