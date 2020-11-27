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
package org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.runtime.AzureDlsGen2ContainerExistRuntime;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;
import org.talend.daikon.runtime.RuntimeInfo;

public class TAzureDlsGen2ContainerExistDefinition extends AzureDlsGen2ContainerDefinition {

    public static final String COMPONENT_NAME = "tAzureDlsGen2ContainerExist"; //$NON-NLS-1$

    public static final String RETURN_CONTAINER_EXIST = "containerExist"; //$NON-NLS-1$

    public static final Property<Boolean> RETURN_CONTAINER_EXIST_PROP = PropertyFactory
            .newBoolean(RETURN_CONTAINER_EXIST);

    public TAzureDlsGen2ContainerExistDefinition() {
        super(COMPONENT_NAME);
        setupI18N(new Property<?>[]{ RETURN_CONTAINER_EXIST_PROP });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Property[] getReturnProperties() {
        return new Property[]{ RETURN_ERROR_MESSAGE_PROP, RETURN_CONTAINER_PROP, RETURN_CONTAINER_EXIST_PROP };
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TAzureDlsGen2ContainerExistProperties.class;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Class getPropertiesClass() {
        return TAzureDlsGen2ContainerExistProperties.class;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties,
                                      ConnectorTopology connectorTopology) {
        assertConnectorTopologyCompatibility(connectorTopology);
        assertEngineCompatibility(engine);
        return getCommonRuntimeInfo(this.getClass().getClassLoader(), AzureDlsGen2ContainerExistRuntime.class);
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.NONE);
    }

}
