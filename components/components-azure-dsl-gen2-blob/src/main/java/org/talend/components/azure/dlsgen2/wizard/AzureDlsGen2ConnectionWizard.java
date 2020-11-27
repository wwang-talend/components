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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;

public class AzureDlsGen2ConnectionWizard extends ComponentWizard {

    TAzureDlsGen2ConnectionProperties cProperties;

    AzureDlsGen2ComponentListProperties qProperties;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ConnectionWizard.class);

    public AzureDlsGen2ConnectionWizard(ComponentWizardDefinition definition, String repositoryLocation) {
        super(definition, repositoryLocation);

        cProperties = new TAzureDlsGen2ConnectionProperties("connection");
        cProperties.init();
        cProperties.setRepositoryLocation(repositoryLocation);
        addForm(cProperties.getForm(TAzureDlsGen2ConnectionProperties.FORM_WIZARD));

        qProperties = new AzureDlsGen2ComponentListProperties("qProperties").setConnection(cProperties)
                .setRepositoryLocation(repositoryLocation);
        qProperties.init();

        addForm(qProperties.getForm(AzureDlsGen2ComponentListProperties.FORM_CONTAINER));
    }

    public void setupProperties(TAzureDlsGen2ConnectionProperties properties) {
        cProperties.setupProperties();
        cProperties.copyValuesFrom(properties);
        if (properties.BlobSchema != null) {
            qProperties.selectedContainerNames.setStoredValue(properties.BlobSchema);
        }

        qProperties.setConnection(cProperties);
    }

}
