// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.azure.dlsgen2.RuntimeContainerMock;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.property.StringProperty;

public class AzureDlsGen2RuntimeTest {

    public static final String PROP_CONNECTION = "PROP_CONNECTION";

    private RuntimeContainer runtimeContainer;

    private TAzureDlsGen2ConnectionProperties properties;

    private AzureDlsGen2Runtime azureDlsGen2Runtime;

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2RuntimeTest.class);

    @Before
    public void setup() {
        runtimeContainer = new RuntimeContainerMock();
        this.azureDlsGen2Runtime = new AzureDlsGen2Runtime();
        properties = new TAzureDlsGen2ConnectionProperties(PROP_CONNECTION);
        properties.setupProperties();
    }

    @Test
    public void testInitializeInvalidAccountNameAndKey() {
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptyKey"), validationResult.getMessage());
    }

    @Test
    public void testInitializeInvalidAccountEmptyName() {
        properties.accountName.setValue("");
        properties.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptyKey"), validationResult.getMessage());
    }

    @Test
    public void testInitializeInvalidAccountEmptyKey() {
        properties.accountName.setValue("fakeAccountName");
        properties.accountKey.setValue("");
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptyKey"), validationResult.getMessage());
    }

    @Test
    public void testInitializeInvalidSas() {
        properties.useSharedAccessSignature.setValue(true);
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptySAS"), validationResult.getMessage());
    }

    @Test
    public void testInitializeValidSas() {
        properties.useSharedAccessSignature.setValue(true);
        properties.sharedAccessSignature.setValue(
                "https://storageaccount.blob.core.windows.net/?sv=2016-05-31&ss=bfqt&srt=sco&sp=rwdlacup&se=2017-06-21T00:59:24Z&st=2017-06-20T16:59:24Z&spr=https&sig=ySDpauCwWFKZqU04n2ch%2BBtN0GajZWrNqW9GIxOfdgU%3D");
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.OK, validationResult.getStatus());
    }

    @Test
    public void testInitializeValidSasCN() {
        properties.useSharedAccessSignature.setValue(true);
        properties.sharedAccessSignature.setValue(
                "https://storageaccount.blob.core.chinacloudapi.cn/sastest?st=2019-08-27T07%3A12%3A27Z&se=2019-08-28T07%3A12%3A27Z&sp=rl&sv=2017-07-29&sr=c&sig=r0UYBj7NL6FQ0YVzU7WjuojHPn0q%2B3hhQp5yAkp7aGQ%3D");
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.OK, validationResult.getStatus());
    }

    @Test
    public void testInitializeValidAccountNameAndKey() {
        properties.accountName.setValue("fakeAccountName");
        properties.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        assertNotNull(azureDlsGen2Runtime.getConnectionProperties());
    }

    @Test
    public void testInitializeValidReferencedConnection() {
        // Init the referenced connection
        TAzureDlsGen2ConnectionProperties referenced = new TAzureDlsGen2ConnectionProperties("sharedConnection");
        referenced.setupProperties();
        referenced.accountName.setValue("fakeAccountName");
        referenced.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");

        // Add referenced connection to the runtime container
        runtimeContainer
                .setComponentData("shared-connection", AzureDlsGen2Runtime.KEY_CONNECTION_PROPERTIES, referenced);

        // reference connection by id
        properties.referencedComponent.componentInstanceId = new StringProperty("connection")
                .setValue("shared-connection");

        // init and test
        ValidationResult validationResult = this.azureDlsGen2Runtime.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        assertNotNull(azureDlsGen2Runtime.getConnectionProperties());
    }

}
