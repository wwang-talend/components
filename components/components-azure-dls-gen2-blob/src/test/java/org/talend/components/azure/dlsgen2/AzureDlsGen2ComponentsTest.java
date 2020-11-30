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
package org.talend.components.azure.dlsgen2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.EnumSet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerProperties;
import org.talend.components.azure.dlsgen2.blob.runtime.AzureDlsGen2ContainerRuntime;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListProperties;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionDefinition;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2ComponentsTest {// extends AzureDlsGen2GenericBase {

    protected RuntimeContainer runtime;

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    @Test
    public void testFamily() {
        TAzureDlsGen2ConnectionDefinition aconn = new TAzureDlsGen2ConnectionDefinition();
        Assert.assertEquals(1, aconn.getFamilies().length);
        Assert.assertEquals("Cloud/Azure Storage/Blob DLS Gen2", aconn.getFamilies()[0]);
    }

    public void checkContainer(AzureDlsGen2ContainerProperties props, String mycontainer) {
        props.container.setValue(mycontainer);
        assertEquals(mycontainer, props.container.getValue());
        assertTrue(true);
    }

    /**
     * Check the container's validation chain
     */
    public ValidationResult getContainerValidation(String container, AzureDlsGen2ContainerProperties properties) {
        properties.container.setValue(container);
        AzureDlsGen2ContainerRuntime sos = new AzureDlsGen2ContainerRuntime();
        return sos.initialize(runtime, properties);
    }

    @Test
    public void testBlobListSchema() {
        Schema s = SchemaBuilder.record("Main").fields().name("BlobName")
                .prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "300")// $NON-NLS-3$
                .prop(SchemaConstants.TALEND_IS_LOCKED, "true").type(AvroUtils._string()).noDefault().endRecord();
        TAzureDlsGen2ListProperties props = new TAzureDlsGen2ListProperties("tests");
        props.setupProperties();
        Schema ps = props.schema.schema.getValue();
        assertEquals(s, ps);
    }

    @Test
    public void testContainerListSchema() {
        Schema s = SchemaBuilder.record("Main").fields().name("ContainerName")
                .prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "50")// $NON-NLS-3$
                .prop(SchemaConstants.TALEND_IS_LOCKED, "true").type(AvroUtils._string()).noDefault().endRecord();
        TAzureDlsGen2ContainerListProperties props = new TAzureDlsGen2ContainerListProperties("tests");
        props.setupProperties();
        Schema ps = props.schema.schema.getValue();
        assertEquals(s, ps);
    }

    @Test
    public void testAzureStorageDefinition() {
        AzureDlsGen2Definition def = new TAzureDlsGen2ConnectionDefinition();
        assertEquals(EnumSet.of(ConnectorTopology.NONE), def.getSupportedConnectorTopologies());
        assertTrue(def.isStartable());
    }

    @Test
    public void testAzureStorageBaseProperties() {
        AzureDlsGen2Properties p = new TAzureDlsGen2ContainerListProperties("test");
        assertEquals(Collections.emptySet(), p.getAllSchemaPropertiesConnectors(false));
        assertEquals(Collections.singleton(new PropertyPathConnector(Connector.MAIN_NAME, "schema")),
                     p.getAllSchemaPropertiesConnectors(true));
    }

}
