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

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.newProperty;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.reflect.TypeLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ProvideConnectionProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.service.Repository;

public class AzureDlsGen2ComponentListProperties extends ComponentPropertiesImpl
        implements AzureDlsGen2ProvideConnectionProperties {

    public static final String FORM_CONTAINER = "Container";

    private static final long serialVersionUID = 1962464678283327395L;

    private TAzureDlsGen2ConnectionProperties connection = new TAzureDlsGen2ConnectionProperties("connection");

    private String repositoryLocation;

    public Property<List<NamedThing>> selectedContainerNames = newProperty(new TypeLiteral<List<NamedThing>>() {
    }, "selectedContainerNames"); //$NON-NLS-1$

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ComponentListProperties.class);

    public AzureDlsGen2ComponentListProperties(String name) {
        super(name);
    }

    @Override
    public TAzureDlsGen2ConnectionProperties getConnectionProperties() {
        return connection;
    }

    public AzureDlsGen2ComponentListProperties setConnection(TAzureDlsGen2ConnectionProperties connection) {
        this.connection = connection;
        return this;
    }

    public AzureDlsGen2ComponentListProperties setRepositoryLocation(String repositoryLocation) {
        this.repositoryLocation = repositoryLocation;
        return this;
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form containerForm = Form.create(this, FORM_CONTAINER);
        containerForm.addRow(widget(selectedContainerNames).setWidgetType(Widget.NAME_SELECTION_AREA_WIDGET_TYPE));
        refreshLayout(containerForm);

    }

    public void beforeFormPresentContainer() {
        selectedContainerNames.setPossibleValues(connection.BlobSchema);
        getForm(FORM_CONTAINER).setAllowBack(true);
        getForm(FORM_CONTAINER).setAllowForward(true);
        getForm(FORM_CONTAINER).setAllowFinish(true);
    }

    public ValidationResult afterFormFinishTable(Repository<Properties> repo) throws Exception {

        connection.BlobSchema = selectedContainerNames.getValue();
        String repoLoc = repo.storeProperties(connection, connection.name.getValue(), repositoryLocation, null);

        String storeId;
        if (selectedContainerNames.getValue() != null) {
            for (NamedThing nl : selectedContainerNames.getValue()) {
                String containerId = nl.getName();
                AzureDlsGen2ContainerProperties containerProps = new AzureDlsGen2ContainerProperties(containerId);
                containerProps.init();
                containerProps.connection = connection;
                containerProps.container.setValue(containerId);
                containerProps.schema.schema.setValue(getContainerSchema());
                repo.storeProperties(containerProps, formatSchemaName(containerId), repoLoc, "schema.schema");
            }
        }
        return ValidationResult.OK;
    }

    private String formatSchemaName(String name) {
        String storeId = name.replaceAll("-", "_").replaceAll(" ", "_");
        if (Character.isDigit(storeId.charAt(0))) {
            storeId = "_" + storeId;
        }
        return storeId;
    }

    public Schema getContainerSchema() {
        return SchemaBuilder.builder().record("Main").fields()//
                .name("containerName").prop(SchemaConstants.TALEND_COLUMN_IS_KEY, "true")
                .prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "100").type(AvroUtils._string()).noDefault()//
                .endRecord();
    }

}
