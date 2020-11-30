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

import org.osgi.service.component.annotations.Component;
import org.talend.components.api.AbstractComponentFamilyDefinition;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.Constants;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerdelete.TAzureDlsGen2ContainerDeleteDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist.TAzureDlsGen2ContainerExistDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestorageget.TAzureDlsGen2GetDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestorageput.TAzureDlsGen2PutDefinition;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionDefinition;
import org.talend.components.azure.dlsgen2.wizard.AzureDlsGen2ConnectionEditWizardDefinition;
import org.talend.components.azure.dlsgen2.wizard.AzureDlsGen2ConnectionWizardDefinition;

/**
 * Install all of the definitions provided for the Azure DLS Gen2 family of components.
 */
@Component(name = Constants.COMPONENT_INSTALLER_PREFIX + AzureDlsGen2FamilyDefinition.NAME, service = ComponentInstaller.class)
public class AzureDlsGen2FamilyDefinition extends AbstractComponentFamilyDefinition implements ComponentInstaller {

    public static final String NAME = "Azure DLS Gen2"; //$NON-NLS-1$

    public AzureDlsGen2FamilyDefinition() {
        super(NAME, new TAzureDlsGen2ConnectionDefinition(),
              // containers and blobs
              new TAzureDlsGen2ContainerExistDefinition(), new TAzureDlsGen2ContainerCreateDefinition(),
              new TAzureDlsGen2ContainerDeleteDefinition(), new TAzureDlsGen2ContainerListDefinition(),
              new TAzureDlsGen2ListDefinition(), new TAzureDlsGen2DeleteDefinition(), new TAzureDlsGen2GetDefinition(),
              new TAzureDlsGen2PutDefinition(),
              // wizards
              new AzureDlsGen2ConnectionWizardDefinition(), new AzureDlsGen2ConnectionEditWizardDefinition()
              //
        );
    }

    @Override
    public void install(ComponentFrameworkContext ctx) {
        ctx.registerComponentFamilyDefinition(this);
    }
}