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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.azure.dlsgen2.RuntimeContainerMock;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.tazurestoragedelete.TAzureDlsGen2DeleteProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties.Protocol;
import org.talend.daikon.properties.ValidationResult;

public class AzureStorageDeleteRuntimeTest {

    public static final String PROP_ = "PROP_";

    private RuntimeContainer runtimeContainer;

    private TAzureDlsGen2DeleteProperties properties;

    private AzureDlsGen2DeleteRuntime deleteBlock;

    @Mock
    private AzureDlsGen2BlobService blobService;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setup() {
        properties = new TAzureDlsGen2DeleteProperties(PROP_ + "DeleteBlock");
        properties.setupProperties();
        // valid connection
        properties.connection = new TAzureDlsGen2ConnectionProperties(PROP_ + "Connection");
        properties.connection.protocol.setValue(Protocol.HTTP);
        properties.connection.accountName.setValue("fakeAccountName");
        properties.connection.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");

        properties.container.setValue("valide-container-name-1");

        runtimeContainer = new RuntimeContainerMock();
        this.deleteBlock = new AzureDlsGen2DeleteRuntime();
    }

    @Test
    public void testInitializeWithEmptyPrefix() {
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
    }

    @Test
    public void testInitializeValid() {
        properties.remoteBlobs.include.setValue(Arrays.asList(true, false));
        properties.remoteBlobs.prefix.setValue(Arrays.asList("block1", "block2"));
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
    }

    @Test
    public void testRunAtDriverNotSuccessfullyDeleted() {
        properties.remoteBlobs.include.setValue(Arrays.asList(true));
        properties.remoteBlobs.prefix.setValue(Arrays.asList("block1"));
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        deleteBlock.azureDlsGen2BlobService = blobService;

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

            // when(blobService.deleteBlob(any(String.class),any(String.class))).thenReturn(false);
            deleteBlock.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }
    }

    @Test
    public void testRunAtDriverValid() {
        properties.remoteBlobs.include.setValue(Arrays.asList(true));
        properties.remoteBlobs.prefix.setValue(Arrays.asList("block1"));
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        deleteBlock.azureDlsGen2BlobService = blobService;
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

            //when(blobService.deleteBlob(any(String.class),any(String.class))).then(invocationOnMock -> {});

            deleteBlock.runAtDriver(runtimeContainer);
        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }

    }

    @Test
    public void testRunAtDriverHandleError() {
        properties.remoteBlobs.include.setValue(Arrays.asList(true));
        properties.remoteBlobs.prefix.setValue(Arrays.asList("block1"));
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        deleteBlock.azureDlsGen2BlobService = blobService;

        try {
            when(blobService.listBlobs(anyString(), anyString(), anyBoolean()))
                    .thenThrow(new BlobStorageException("dummy message", null, new RuntimeException()));
            deleteBlock.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }
    }

    @Test(expected = ComponentException.class)
    public void testRunAtDriverDieOnError() {
        properties.remoteBlobs.include.setValue(Arrays.asList(true));
        properties.remoteBlobs.prefix.setValue(Arrays.asList("block1"));
        properties.dieOnError.setValue(true);
        ValidationResult validationResult = deleteBlock.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        deleteBlock.azureDlsGen2BlobService = blobService;
        // prepare test data and mocks
        try {
            when(blobService.listBlobs(anyString(), anyString(), anyBoolean()))
                    .thenThrow(new BlobStorageException("dummy message", null, new RuntimeException()));
            deleteBlock.runAtDriver(runtimeContainer);
        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }
    }

}
