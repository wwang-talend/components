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
package org.talend.components.azure.dlsgen2.blob.runtime.it;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.components.azure.dlsgen2.AzureDlsGen2BaseTestIT;
import org.talend.components.azure.dlsgen2.blob.runtime.AzureDlsGen2SourceOrSink;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;

/**
 *
 */
@Ignore
public class AzureDlsGen2SourceOrSinkTestIT extends AzureDlsGen2BaseTestIT {

    public AzureDlsGen2SourceOrSinkTestIT() {
        super("source-or-sink");
    }

    @Before
    public void setUp() {

    }

    @Test(expected = Exception.class)
    public void testInvalidConnection() throws Throwable {
        // fail("Not yet implemented");
        AzureDlsGen2SourceOrSink sos = new AzureDlsGen2SourceOrSink();
        TAzureDlsGen2ConnectionProperties properties = new TAzureDlsGen2ConnectionProperties("tests");
        properties.setupProperties();
        sos.initialize(runtime, properties);
    }
}
