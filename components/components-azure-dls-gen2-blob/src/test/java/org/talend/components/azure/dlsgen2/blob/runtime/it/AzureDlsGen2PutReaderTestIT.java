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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainercreate.TAzureDlsGen2ContainerCreateProperties.AccessControl;
import org.talend.components.azure.dlsgen2.blob.tazurestorageput.TAzureDlsGen2PutProperties;

@Ignore
public class AzureDlsGen2PutReaderTestIT extends AzureDlsGen2BaseBlobTestIT {

    private String CONTAINER;

    private TAzureDlsGen2PutProperties properties;

    public AzureDlsGen2PutReaderTestIT() {
        super("put-" + getRandomTestUID());
        CONTAINER = getNamedThingForTest(TEST_CONTAINER_1);
        properties = new TAzureDlsGen2PutProperties("tests");
        properties.container.setValue(CONTAINER);
        setupConnectionProperties(properties);
        properties.localFolder.setValue(FOLDER_PATH_PUT);
        properties.remoteFolder.setValue("");
    }

    @Before
    public void createTestBlobs() throws Exception {
        doContainerCreate(CONTAINER, AccessControl.Private);
    }

    @After
    public void cleanupTestBlobs() throws Exception {
        doContainerDelete(CONTAINER);
    }

    @SuppressWarnings({ "unused", "rawtypes" })
    @Test
    public void testBlobPutFolder() throws Exception {
        properties.useFileList.setValue(false);
        BoundedReader reader = createBoundedReader(properties);
        assertTrue(reader.start());
        List<String> blobs = listAllBlobs(CONTAINER);

    }

}
