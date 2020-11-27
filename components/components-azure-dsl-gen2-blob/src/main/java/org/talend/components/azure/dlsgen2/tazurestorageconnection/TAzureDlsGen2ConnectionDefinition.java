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
package org.talend.components.azure.dlsgen2.tazurestorageconnection;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.AzureDlsGen2Definition;
import org.talend.components.azure.dlsgen2.blob.runtime.AzureDlsGen2SourceOrSink;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.runtime.RuntimeInfo;

/**
 * @author undx
 */
public class TAzureDlsGen2ConnectionDefinition extends AzureDlsGen2Definition {

    public static final String COMPONENT_NAME = "tAzureDlsGen2Connection"; //$NON-NLS-1$

    public TAzureDlsGen2ConnectionDefinition() {
        super(COMPONENT_NAME);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TAzureDlsGen2ConnectionProperties.class;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Class getPropertiesClass() {
        return TAzureDlsGen2ConnectionProperties.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Property[] getReturnProperties() {
        return new Property[]{};
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties,
                                      ConnectorTopology connectorTopology) {
        if (connectorTopology == ConnectorTopology.NONE) {
            return getCommonRuntimeInfo(this.getClass().getClassLoader(), AzureDlsGen2SourceOrSink.class);
        } else {
            return null;
        }
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.NONE);
    }
}
