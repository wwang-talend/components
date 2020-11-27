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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.azure.storage.blob.BlobServiceClient;

import org.apache.avro.generic.IndexedRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.azure.dlsgen2.AzureDlsGen2ConnectionWithKeyService;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlobsTable;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListProperties;

@Ignore
public class AzureDlsGen2ListReaderTestIT extends AzureDlsGen2BaseBlobTestIT {

    private String CONTAINER;

    private TAzureDlsGen2ListProperties properties;

    private RemoteBlobsTable remoteBlobs;

    private List<String> blobs;

    private List<String> prefixes;

    private List<Boolean> includes;

    public AzureDlsGen2ListReaderTestIT() {
        super("list-" + getRandomTestUID());
        CONTAINER = getNamedThingForTest(TEST_CONTAINER_1);
        properties = new TAzureDlsGen2ListProperties("tests");
        properties.container.setValue(CONTAINER);
        setupConnectionProperties(properties);
        remoteBlobs = new RemoteBlobsTable("tests");
        blobs = new ArrayList<>();
        prefixes = new ArrayList<>();
        includes = new ArrayList<>();
    }

    @Before
    public void createTestBlobs() throws Exception {
        uploadTestBlobs(CONTAINER);
    }

    @After
    public void cleanupTestBlobs() throws Exception {
        doContainerDelete(CONTAINER);
    }

    @SuppressWarnings("rawtypes")
    public void listBlobs() throws Exception {
        blobs.clear();
        properties.remoteBlobs = remoteBlobs;
        properties.schema.schema.setValue(schemaForBlobList);
        BoundedReader reader = createBoundedReader(properties);
        Object row;
        Boolean rows = reader.start();
        while (rows) {
            row = ((IndexedRecord) reader.getCurrent()).get(0);
            assertNotNull(row);
            assertTrue(row instanceof String);
            blobs.add(row.toString());
            rows = reader.advance();
        }
        reader.close();
    }

    @Test
    public void testBlobListAll() throws Exception {
        prefixes.clear();
        includes.clear();
        prefixes.add("");
        includes.add(true);
        remoteBlobs.prefix.setValue(prefixes);
        remoteBlobs.include.setValue(includes);
        listBlobs();
        for (String blob : TEST_ALL_BLOBS)
            assertTrue(isInBlobList(blob, blobs));
    }

    @Test
    public void testBlobListRootOnly() throws Exception {
        prefixes.clear();
        includes.clear();
        prefixes.add("");
        includes.add(false);
        remoteBlobs.prefix.setValue(prefixes);
        remoteBlobs.include.setValue(includes);
        listBlobs();
        for (String blob : TEST_ROOT_BLOBS)
            assertTrue(isInBlobList(blob, blobs));
        // sub1 blobs shouldn't appear in list...
        for (String blob : TEST_SUB1_BLOBS)
            assertFalse(isInBlobList(blob, blobs));
    }

    @Test
    public void testBlobListSub1() throws Exception {
        prefixes.clear();
        includes.clear();
        prefixes.add("sub1/");
        includes.add(false);
        remoteBlobs.prefix.setValue(prefixes);
        remoteBlobs.include.setValue(includes);
        listBlobs();
        for (String blob : TEST_SUB1_BLOBS)
            assertTrue(isInBlobList(blob, blobs));
    }

    @Test
    public void testConnection() {
        AzureDlsGen2ConnectionWithKeyService connection = AzureDlsGen2ConnectionWithKeyService.builder()//
                .accountName(accountName)//
                .accountKey(accountKey)
                .build();
        final BlobServiceClient blobClient = connection.getBlobServiceClient();
    }

}
