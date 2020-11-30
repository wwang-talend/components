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
package org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerdelete;

import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

public class TAzureDlsGen2ContainerDeleteProperties extends AzureDlsGen2ContainerProperties {

    private static final long serialVersionUID = -8409678756536686919L;

    public Property<Boolean> dieOnError = PropertyFactory.newBoolean("dieOnError");

    public TAzureDlsGen2ContainerDeleteProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = getForm(Form.MAIN);
        mainForm.addRow(dieOnError);
    }
}
