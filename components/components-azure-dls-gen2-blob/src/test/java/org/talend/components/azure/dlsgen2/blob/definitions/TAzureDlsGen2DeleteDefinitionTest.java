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
package org.talend.components.azure.dlsgen2.blob.definitions;

import static org.junit.Assert.assertNotNull;

import java.util.EnumSet;

import org.junit.Assert;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteProperties;

public class TAzureDlsGen2DeleteDefinitionTest extends AzureDlsGen2ContainerDefinitionTest {

    @Override
    public void setUp() throws Exception {
        azureStorageContainerDefinition = new TAzureDlsGen2DeleteDefinition();
    }

    @Override
    public void testGetPropertiesClass() {
        Assert.assertEquals(TAzureDlsGen2DeleteProperties.class, azureStorageContainerDefinition.getPropertiesClass());
    }

    @Override
    public void testGetSupportedConnectorTopologies() {
        Assert.assertEquals(EnumSet.of(ConnectorTopology.NONE), azureStorageContainerDefinition
                .getSupportedConnectorTopologies());

    }

    @Override
    public void testGetRuntimeInfo() {
        assertNotNull(azureStorageContainerDefinition.getRuntimeInfo(ExecutionEngine.DI, null, ConnectorTopology.NONE));
    }

    @Override
    public void testGetReturnProperties() {
        assertNotNull(azureStorageContainerDefinition.getReturnProperties());
        Assert.assertEquals(2, azureStorageContainerDefinition.getReturnProperties().length);
    }

}
