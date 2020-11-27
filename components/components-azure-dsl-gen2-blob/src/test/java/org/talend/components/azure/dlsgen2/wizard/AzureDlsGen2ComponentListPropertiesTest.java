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
package org.talend.components.azure.dlsgen2.wizard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.service.Repository;

public class AzureDlsGen2ComponentListPropertiesTest extends AzureDlsGen2ConnectionWizardTest {

    private AzureDlsGen2ComponentListProperties properties;

    private final List<RepoProps> repoProps = new ArrayList<>();

    private Repository<Properties> repo = new TestRepository(repoProps);

    Schema schemaContainer = SchemaBuilder.builder().record("Main").fields()//
            .name("containerName").prop(SchemaConstants.TALEND_COLUMN_IS_KEY, "true")
            .prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "100").type(AvroUtils._string()).noDefault()//
            .endRecord();

    @Before
    public void setUp() throws Exception {
        properties = new AzureDlsGen2ComponentListProperties("test");
        // final List<RepoProps> repoProps = new ArrayList<>();
        // Repository repo = new TestRepository(repoProps);
        // properties.setConnection(null);
    }

    /**
     * @see AzureDlsGen2ComponentListProperties#getContainerSchema()
     */
    @Test
    public void getContainerSchema() {
        Schema containerschema = properties.getContainerSchema();
        assertNotNull("containerschema cannot be null", containerschema);
    }

    /**
     * @see AzureDlsGen2ComponentListProperties#setConnection(TAzureDlsGen2ConnectionProperties)
     */
    @Test
    public void testSetConnection() {
        TAzureDlsGen2ConnectionProperties connection = new TAzureDlsGen2ConnectionProperties(null);
        AzureDlsGen2ComponentListProperties result = properties.setConnection(connection);
        assertNotNull("result cannot be null", result.getConnectionProperties());
    }

    /**
     * @see AzureDlsGen2ComponentListProperties#getContainerSchema()
     */
    @Test
    public void testGetContainerSchema() {
        Schema containerschema = properties.getContainerSchema();
        assertNotNull("containerschema cannot be null", containerschema);
        assertEquals(schemaContainer, containerschema);
    }

    /**
     * @see AzureDlsGen2ComponentListProperties#getConnectionProperties()
     */
    @Test
    public void testGetConnectionProperties() {
        assertNotNull(properties.getConnectionProperties());
        properties.setConnection(null);
        assertNull(properties.getConnectionProperties());
    }

    /**
     * @see AzureDlsGen2ComponentListProperties#beforeFormPresentContainer()
     */
    @Test
    public void testBeforeFormPresentContainer() throws Exception {
        properties.setupLayout();
        properties.beforeFormPresentContainer();
        assertTrue("Shoud be ture", properties.getForm(AzureDlsGen2ComponentListProperties.FORM_CONTAINER)
                .isAllowBack() &&
                properties.getForm(AzureDlsGen2ComponentListProperties.FORM_CONTAINER).isAllowForward() &&
                properties.getForm(AzureDlsGen2ComponentListProperties.FORM_CONTAINER).isAllowFinish());
    }

}
