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

import javax.inject.Inject;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.components.api.test.AbstractComponentTest2;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerdelete.TAzureDlsGen2ContainerDeleteDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist.TAzureDlsGen2ContainerExistDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestorageget.TAzureDlsGen2GetDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestorageput.TAzureDlsGen2PutDefinition;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionDefinition;
import org.talend.daikon.definition.service.DefinitionRegistryService;

public abstract class AzureDlsGen2GenericBase extends AbstractComponentTest2 {

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    @Inject
    DefinitionRegistry testComponentRegistry;

    @Override
    public DefinitionRegistryService getDefinitionRegistry() {
        if (testComponentRegistry == null) {
            testComponentRegistry = new DefinitionRegistry();

            testComponentRegistry.registerComponentFamilyDefinition(new AzureDlsGen2FamilyDefinition());
        }
        return testComponentRegistry;
    }

    // @Inject
    // ComponentService compServ;

    private ComponentServiceImpl componentService;

    public ComponentService getComponentService() {
        // return compServ;
        if (componentService == null) {
            DefinitionRegistry testComponentRegistry = new DefinitionRegistry();

            testComponentRegistry.registerComponentFamilyDefinition(new AzureDlsGen2FamilyDefinition());
            componentService = new ComponentServiceImpl(testComponentRegistry);
        }
        return componentService;
    }

    @Test
    public void testAllComponentsAreRegistered() {
        // blobs
        assertComponentIsRegistered(TAzureDlsGen2ConnectionDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2ContainerCreateDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2ContainerDeleteDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2ContainerExistDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2ContainerListDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2DeleteDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2GetDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2ListDefinition.COMPONENT_NAME);
        assertComponentIsRegistered(TAzureDlsGen2PutDefinition.COMPONENT_NAME);
    }
}
