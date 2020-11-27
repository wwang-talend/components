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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateProperties.AccessControl;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListDefinition;

public abstract class AzureDlsGen2ContainerDefinitionTest {

    protected AzureDlsGen2ContainerDefinition azureStorageContainerDefinition;

    /**
     * initialize azureStorageContainerDefinition
     */
    @Before
    abstract public void setUp() throws Exception;

    @Test
    abstract public void testGetPropertiesClass();

    @Test
    abstract public void testGetSupportedConnectorTopologies();

    @Test
    public abstract void testGetRuntimeInfo();

    @Test
    public void testGetNestedCompatibleComponentPropertiesClass() {
        assertNotNull(azureStorageContainerDefinition.getNestedCompatibleComponentPropertiesClass());
    }

    @Test
    public void testGetFamilies() {
        assertNotNull(azureStorageContainerDefinition.getFamilies());
    }

    @Test
    public abstract void testGetReturnProperties();

    @Test
    public void testIsSchemAutoPropagate() {
        assertTrue(new TAzureDlsGen2ContainerListDefinition().isSchemaAutoPropagate());
        assertTrue(new TAzureDlsGen2ListDefinition().isSchemaAutoPropagate());
    }

    @Test
    public void testEnums() {
        Assert.assertEquals(AccessControl.Private, AccessControl.valueOf("Private"));
        assertEquals(AccessControl.Public, AccessControl.valueOf("Public"));
    }

}
