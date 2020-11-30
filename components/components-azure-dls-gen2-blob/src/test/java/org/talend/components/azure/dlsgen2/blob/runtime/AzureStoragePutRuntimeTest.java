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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.azure.dlsgen2.RuntimeContainerMock;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.helpers.FileMaskTable;
import org.talend.components.azure.dlsgen2.blob.tazurestorageput.TAzureDlsGen2PutProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties.Protocol;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureStoragePutRuntimeTest {

    public static final String PROP_ = "PROP_";

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureStorageGetRuntimeTest.class);

    private RuntimeContainer runtimeContainer;

    private TAzureDlsGen2PutProperties properties;

    private AzureDlsGen2PutRuntime storagePut;

    @Mock
    private AzureDlsGen2BlobService blobService;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private String localFolderPath;

    private BlobStorageException storageException;

    @Before
    public void setup() throws IOException {
        properties = new TAzureDlsGen2PutProperties(PROP_ + "Put");
        properties.setupProperties();
        // valid connection
        properties.connection = new TAzureDlsGen2ConnectionProperties(PROP_ + "Connection");
        properties.connection.protocol.setValue(Protocol.HTTP);
        properties.connection.accountName.setValue("fakeAccountName");
        properties.connection.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");
        properties.container.setValue("goog-container-name-1");

        runtimeContainer = new RuntimeContainerMock();
        this.storagePut = new AzureDlsGen2PutRuntime();

        localFolderPath = getClass().getClassLoader().getResource("azurestorage-put").getPath();

        storageException = new BlobStorageException("some storage exception",
                                                    new HttpResponse(null) {

                                                        @Override
                                                        public int getStatusCode() {
                                                            return 500;
                                                        }

                                                        @Override
                                                        public String getHeaderValue(String name) {
                                                            return "headers.getValue(name)";
                                                        }

                                                        @Override
                                                        public HttpHeaders getHeaders() {
                                                            Map<String, String> headers = new HashMap<>();
                                                            headers.put("x-ms-error-code", "500");
                                                            return new HttpHeaders(headers);
                                                        }

                                                        @Override
                                                        public Flux<ByteBuffer> getBody() {
                                                            return Flux.empty();
                                                        }

                                                        @Override
                                                        public Mono<byte[]> getBodyAsByteArray() {
                                                            return Mono.just(new byte[0]);
                                                        }

                                                        @Override
                                                        public Mono<String> getBodyAsString() {
                                                            return Mono.just("");
                                                        }

                                                        @Override
                                                        public Mono<String> getBodyAsString(Charset charset) {
                                                            return Mono.just("");
                                                        }

                                                    }, Collections.singletonMap("Message", "error"));
    }

    @Test
    public void testEmptyLocalFolder() {
        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptyLocalFolder"), validationResult.getMessage());
    }

    @Test
    public void testEmptyFileList() {
        properties.localFolder.setValue(localFolderPath);
        properties.useFileList.setValue(true);
        properties.files = new FileMaskTable("fileMaskTable");
        properties.files.fileMask.setValue(new ArrayList<String>());

        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
        assertEquals(messages.getMessage("error.EmptyFileList"), validationResult.getMessage());
    }

    @Test
    public void testValidProperties() {
        properties.localFolder.setValue(localFolderPath);
        properties.useFileList.setValue(true);
        properties.files = new FileMaskTable("fileMaskTable");
        properties.files.fileMask.setValue(new ArrayList<String>());
        properties.files.newName.setValue(new ArrayList<String>());

        properties.files.fileMask.getValue().add("blob1*");
        properties.files.newName.getValue().add("blob");

        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertNull(validationResult.getMessage());
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
    }

    @Test
    public void testRunAtDriverHandleStorageException() {

        properties.localFolder.setValue(localFolderPath);
        properties.useFileList.setValue(true);
        properties.files = new FileMaskTable("fileMaskTable");
        properties.files.fileMask.setValue(new ArrayList<String>());
        properties.files.newName.setValue(new ArrayList<String>());
        properties.files.fileMask.getValue().add("blob1*");
        properties.files.newName.getValue().add("blob");

        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());

        storagePut.azureDlsGen2BlobService = blobService;
        try {
            doAnswer(new Answer<Void>() {

                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    throw storageException;
                }
            }).when(blobService).upload(anyString(), anyString(), any(InputStream.class), anyLong());
            this.storagePut.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }
    }

    @Test(expected = ComponentException.class)
    public void testRunAtDriverHandleDieOnError() {

        properties.localFolder.setValue(localFolderPath);
        properties.useFileList.setValue(true);
        properties.files = new FileMaskTable("fileMaskTable");
        properties.files.fileMask.setValue(new ArrayList<String>());
        properties.files.newName.setValue(new ArrayList<String>());
        properties.files.fileMask.getValue().add("blob1*");
        properties.files.newName.getValue().add("blob");
        properties.dieOnError.setValue(true);

        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());

        storagePut.azureDlsGen2BlobService = blobService;
        try {
            doAnswer(new Answer<Void>() {

                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    throw storageException;
                }
            }).when(blobService).upload(anyString(), anyString(), any(InputStream.class), anyLong());
            this.storagePut.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }

    }

    @Test
    public void testRunAtDriverValid() {

        properties.localFolder.setValue(localFolderPath);
        properties.useFileList.setValue(true);
        properties.files = new FileMaskTable("fileMaskTable");
        properties.files.fileMask.setValue(new ArrayList<String>());
        properties.files.newName.setValue(new ArrayList<String>());
        properties.files.fileMask.getValue().add("blob1*");
        properties.files.newName.getValue().add("blob");

        ValidationResult validationResult = storagePut.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());

        storagePut.azureDlsGen2BlobService = blobService;
        try {
            doAnswer(new Answer<Void>() {

                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    return null;
                }
            }).when(blobService).upload(anyString(), anyString(), any(InputStream.class), anyLong());
            this.storagePut.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw " + e.getMessage());
        }

    }

}
