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

import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;

public interface AzureDlsGen2ProvideConnectionProperties {

    /**
     * getConnectionProperties.
     *
     * @return {@link TAzureDlsGen2ConnectionProperties} properties of connection.
     */
    TAzureDlsGen2ConnectionProperties getConnectionProperties();
}
