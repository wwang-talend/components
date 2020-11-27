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
package org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.azure.dlsgen2.AzureDlsGen2Properties;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 * Class TAzureStorageContainerListProperties.
 *
 * List the Azure storage containers available for the storage account.
 */
public class TAzureDlsGen2ContainerListProperties extends AzureDlsGen2Properties {

    private static final long serialVersionUID = -5146548113930743503L;

    public Property<Boolean> dieOnError = PropertyFactory.newBoolean("dieOnError");

    protected transient PropertyPathConnector MAIN_NAME = new PropertyPathConnector(Connector.MAIN_NAME, "schema");//$NON-NLS-1$

    public TAzureDlsGen2ContainerListProperties(String name) {
        super(name);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();

        Schema s = SchemaBuilder.record("Main").fields()
                //
                .name("ContainerName").prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "50")// $NON-NLS-3$
                .prop(SchemaConstants.TALEND_IS_LOCKED, "true")//$NON-NLS-1$
                //
                .type(AvroUtils._string()).noDefault().endRecord();
        schema.schema.setValue(s);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form main = getForm(Form.MAIN);
        main.addRow(schema.getForm(Form.REFERENCE));
        main.addRow(dieOnError);
    }

}
