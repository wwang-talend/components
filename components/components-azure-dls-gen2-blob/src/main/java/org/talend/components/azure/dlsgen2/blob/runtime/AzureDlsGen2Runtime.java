package org.talend.components.azure.dlsgen2.blob.runtime;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.azure.storage.blob.BlobServiceClient;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.AzureDlsGen2Connection;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ConnectionWithKeyService;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ConnectionWithSasService;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ConnectionWithToken;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ProvideConnectionProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.AuthType;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.components.azure.dlsgen2.utils.AzureDlsGen2Utils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2Runtime implements RuntimableRuntime<ComponentProperties> {

    private static final long serialVersionUID = 8150539704549116311L;

    public static final String KEY_CONNECTION_PROPERTIES = "connection";

    public AzureDlsGen2ProvideConnectionProperties properties;

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2Runtime.class);

    private static final String SAS_PATTERN = "(http.?)?://(.*)\\.(blob|file|queue|table)(.*)/(.*)";

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        // init
        this.properties = (AzureDlsGen2ProvideConnectionProperties) properties;
        TAzureDlsGen2ConnectionProperties conn = getUsedConnection(runtimeContainer);

        if (runtimeContainer != null) {
            AzureDlsGen2Utils.setApplicationVersion((String) runtimeContainer
                    .getGlobalData(AzureDlsGen2Utils.TALEND_PRODUCT_VERSION_GLOBAL_KEY));
            AzureDlsGen2Utils.setComponentVersion((String) runtimeContainer
                    .getGlobalData(AzureDlsGen2Utils.TALEND_COMPONENT_VERSION_GLOBAL_KEY));
        }

        // Validate connection properties

        String errorMessage = "";
        if (conn == null) { // check connection failure

            errorMessage = messages.getMessage("error.VacantConnection"); //$NON-NLS-1$

        } else if (conn.authenticationType.getValue() == AuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL) {
            if (StringUtils.isEmpty(conn.accountName.getValue()) || StringUtils.isEmpty(conn.tenantId.getValue())
                    || StringUtils.isEmpty(conn.clientId.getValue()) || StringUtils
                    .isEmpty(conn.clientSecret.getValue())) {
                errorMessage = messages.getMessage("error.EmptyADProperties");
            }
        } else if (conn.useSharedAccessSignature.getValue()) { // checks
            if (StringUtils.isEmpty(conn.sharedAccessSignature.getStringValue())) {
                errorMessage = messages.getMessage("error.EmptySAS"); //$NON-NLS-1$
            } else {
                Matcher m = Pattern.compile(SAS_PATTERN).matcher(conn.sharedAccessSignature.getValue());
                if (!m.matches()) {
                    errorMessage = messages.getMessage("error.InvalidSAS");
                }
            }

        } else if (!conn.useSharedAccessSignature.getValue() && (StringUtils.isEmpty(conn.accountName.getStringValue())
                || StringUtils.isEmpty(conn.accountKey.getStringValue()))) { // checks connection's account and key

            errorMessage = messages.getMessage("error.EmptyKey"); //$NON-NLS-1$
        }

        // Return result
        if (errorMessage.isEmpty()) {
            return ValidationResult.OK;
        } else {
            return new ValidationResult(ValidationResult.Result.ERROR, errorMessage);
        }
    }

    public TAzureDlsGen2ConnectionProperties getConnectionProperties() {
        return properties.getConnectionProperties();
    }

    public TAzureDlsGen2ConnectionProperties getUsedConnection(RuntimeContainer runtimeContainer) {
        TAzureDlsGen2ConnectionProperties connectionProperties = properties.getConnectionProperties();
        String refComponentId = connectionProperties.getReferencedComponentId();

        // Using another component's connection
        if (refComponentId != null) {
            // In a runtime container
            if (runtimeContainer != null) {
                TAzureDlsGen2ConnectionProperties sharedConn = (TAzureDlsGen2ConnectionProperties) runtimeContainer
                        .getComponentData(refComponentId, KEY_CONNECTION_PROPERTIES);
                if (sharedConn != null) {
                    return sharedConn;
                }
            }
            // Design time
            connectionProperties = connectionProperties.getReferencedConnectionProperties();
        }
        if (runtimeContainer != null) {
            runtimeContainer.setComponentData(runtimeContainer.getCurrentComponentId(), KEY_CONNECTION_PROPERTIES,
                                              connectionProperties);
        }
        return connectionProperties;
    }

    public BlobServiceClient getBlobServiceClient(RuntimeContainer runtimeContainer) {
        return getAzureConnection(runtimeContainer).getBlobServiceClient();
    }

    public AzureDlsGen2Connection getAzureConnection(RuntimeContainer runtimeContainer) {

        TAzureDlsGen2ConnectionProperties conn = getUsedConnection(runtimeContainer);
        if (conn.authenticationType.getValue() == AuthType.BASIC) {
            if (conn.useSharedAccessSignature.getValue()) {
                // extract account name and sas token from sas url
                Matcher m = Pattern.compile(SAS_PATTERN).matcher(conn.sharedAccessSignature.getValue());
                m.matches();

                return AzureDlsGen2ConnectionWithSasService.builder()//
                        .accountName(m.group(2))//
                        .sasToken(m.group(5))//
                        .build();

            } else {

                return AzureDlsGen2ConnectionWithKeyService.builder()//
                        .protocol(conn.protocol.getValue().toString().toLowerCase())//
                        .accountName(conn.accountName.getValue())//
                        .accountKey(conn.accountKey.getValue()).build();
            }

        } else {
            return new AzureDlsGen2ConnectionWithToken(conn.accountName.getValue(), conn.tenantId.getValue(), conn.clientId
                    .getValue(), conn.clientSecret.getValue());
        }
    }
}
