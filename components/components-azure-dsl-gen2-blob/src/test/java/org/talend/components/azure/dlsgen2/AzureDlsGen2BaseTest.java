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
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties.Protocol;

public class AzureDlsGen2BaseTest {

    public static final String PROP_ = "PROP_";

    public TAzureDlsGen2ConnectionProperties getValidFakeConnection() {
        TAzureDlsGen2ConnectionProperties connection = new TAzureDlsGen2ConnectionProperties(PROP_ + "Connection");
        connection.protocol.setValue(Protocol.HTTP);
        connection.accountName.setValue("fakeAccountName");
        connection.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");
        return connection;
    }

    public RuntimeContainerMock getDummyRuntimeContiner() {
        return new RuntimeContainerMock();
    }

}
