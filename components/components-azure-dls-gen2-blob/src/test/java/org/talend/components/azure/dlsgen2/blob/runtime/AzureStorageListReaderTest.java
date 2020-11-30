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
package org.talend.components.azure.dlsgen2.blob.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.azure.dlsgen2.RuntimeContainerMock;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlobsTable;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties.Protocol;

public class AzureStorageListReaderTest {

    private AzureDlsGen2ListReader reader;

    private TAzureDlsGen2ListProperties properties;

    public static final String PROP_ = "PROP_";

    private RuntimeContainer runtimeContainer;

    @Mock
    private AzureDlsGen2BlobService blobService;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setUp() throws Exception {

        runtimeContainer = new RuntimeContainerMock();

        AzureDlsGen2Source source = new AzureDlsGen2Source();
        properties = new TAzureDlsGen2ListProperties(PROP_ + "List");
        properties.connection = new TAzureDlsGen2ConnectionProperties(PROP_ + "Connection");
        properties.connection.protocol.setValue(Protocol.HTTP);
        properties.connection.accountName.setValue("fakeAccountName");
        properties.connection.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");
        properties.setupProperties();

        source.initialize(runtimeContainer, properties);
        reader = (AzureDlsGen2ListReader) source.createReader(runtimeContainer);
        reader.azureDlsGen2BlobService = blobService;
    }

    @Test
    public void testStartAsNonStartable() {
        try {

            when(blobService.listBlobs(anyString(), anyString(), anyBoolean()))
                    .thenReturn(new PagedIterable<BlobItem>(new PagedFlux<BlobItem>(() -> {
                        return null;
                    })) {

                        @Override
                        public Iterator<BlobItem> iterator() {
                            return new DummyListBlobItemIterator(new ArrayList<BlobItem>());
                        }
                    });

            properties.remoteBlobs = new RemoteBlobsTable("RemoteBlobsTable");
            properties.remoteBlobs.include.setValue(Arrays.asList(true));
            properties.remoteBlobs.prefix.setValue(Arrays.asList("dummyFilter"));

            boolean startable = reader.start();
            assertFalse(startable);
            assertFalse(reader.advance());

        } catch (BlobStorageException | IOException e) {
            fail("should not throw " + e.getMessage());
        }
    }

    @Test
    public void testStartAsStartabke() {

        try {
            final List<BlobItem> list = new ArrayList<>();
            list.add(new BlobItem());
            list.add(new BlobItem());
            list.add(new BlobItem());
            when(blobService.listBlobs(anyString(), anyString(), anyBoolean()))
                    .thenReturn(new PagedIterable<BlobItem>(new PagedFlux<BlobItem>(() -> {
                        return null;
                    })) {

                        @Override
                        public Iterator<BlobItem> iterator() {
                            return new DummyListBlobItemIterator(list);
                        }
                    });

            properties.remoteBlobs = new RemoteBlobsTable("RemoteBlobsTable");
            properties.remoteBlobs.include.setValue(Arrays.asList(true));
            properties.remoteBlobs.prefix.setValue(Arrays.asList("someFilter"));

            boolean startable = reader.start();
            assertTrue(startable);
            assertNotNull(reader.getCurrent());
            while (reader.advance()) {
                assertNotNull(reader.getCurrent());
            }
            assertNotNull(reader.getReturnValues());
            assertEquals(3, reader.getReturnValues().get(ComponentDefinition.RETURN_TOTAL_RECORD_COUNT));

        } catch (BlobStorageException | IOException e) {
            fail("should not throw " + e.getMessage());
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void getCurrentOnNonStartableReader() {
        reader.getCurrent();
        fail("should throw NoSuchElementException");
    }

    @Test(expected = NoSuchElementException.class)
    public void getCurrentOnNonAdvancableReader() {
        try {
            final List<BlobItem> list = new ArrayList<>();
            list.add(new BlobItem());

            when(blobService.listBlobs(anyString(), anyString(), anyBoolean()))
                    .thenReturn(new PagedIterable<BlobItem>(new PagedFlux<BlobItem>(() -> {
                        return null;
                    })) {

                        @Override
                        public Iterator<BlobItem> iterator() {
                            return new DummyListBlobItemIterator(list);
                        }
                    });

            properties.remoteBlobs = new RemoteBlobsTable("RemoteBlobsTable");
            properties.remoteBlobs.include.setValue(Arrays.asList(true));
            properties.remoteBlobs.prefix.setValue(Arrays.asList("someFilter"));

            boolean startable = reader.start();
            assertTrue(startable);
            assertNotNull(reader.getCurrent());
            assertFalse(reader.advance());

            reader.getCurrent();
            fail("should throw NoSuchElementException");

        } catch (BlobStorageException | IOException e) {
            fail("should not throw " + e.getMessage());
        }
    }

}
