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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
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
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerexist.TAzureDlsGen2ContainerExistProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties;
import org.talend.components.azure.dlsgen2.tazurestorageconnection.TAzureDlsGen2ConnectionProperties.Protocol;
import org.talend.daikon.properties.ValidationResult;

public class AzureStorageContainerExistRuntimeTest {

    public static final String PROP_ = "PROP_";

    private RuntimeContainer runtimeContainer;

    private TAzureDlsGen2ContainerExistProperties properties;

    private AzureDlsGen2ContainerExistRuntime existContainer;

    private BlobStorageException storageException;

    @Mock
    private AzureDlsGen2BlobService blobService;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setup() {
        properties = new TAzureDlsGen2ContainerExistProperties(PROP_ + "ExistContainer");
        properties.setupProperties();
        // valid connection
        properties.connection = new TAzureDlsGen2ConnectionProperties(PROP_ + "Connection");
        properties.connection.protocol.setValue(Protocol.HTTP);
        properties.connection.accountName.setValue("fakeAccountName");
        properties.connection.accountKey.setValue("fakeAccountKey=ANBHFYRJJFHRIKKJFU");

        runtimeContainer = new RuntimeContainerMock();
        this.existContainer = new AzureDlsGen2ContainerExistRuntime();
        storageException = new BlobStorageException("storage exception message", new HttpResponse(null) {

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

        }, new RuntimeException());
    }

    @Test
    public void testInitializeEmptyContainerName() {
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.Result.ERROR, validationResult.getStatus());
    }

    @Test
    public void testInitializeValide() {
        properties.container.setValue("container");
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
    }

    /**
     * The method {@link AzureDlsGen2ContainerCreateRuntime#runAtDriver(RuntimeContainer)} should not throw any exception if the
     * dieOnError is not set to true.
     */
    @Test
    public void testRunAtDriverHandleStorageException() {

        properties.container.setValue("container-name-ok");
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        existContainer.azureDlsGen2BlobService = blobService;
        // Handle Storage exception
        try {
            when(blobService.containerExist(anyString())).thenThrow(storageException);
            existContainer.runAtDriver(runtimeContainer);

        } catch (BlobStorageException e) {
            fail("should not throw exception " + e.getMessage());
        }
    }

    @Test(expected = ComponentException.class)
    public void testRunAtDriverDieOnError() {

        properties.container.setValue("container-name-ok");
        properties.dieOnError.setValue(true);
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        existContainer.azureDlsGen2BlobService = blobService;

        try {
            when(blobService.containerExist(anyString())).thenThrow(storageException);
            existContainer.runAtDriver(runtimeContainer);
        } catch (BlobStorageException e) {
            fail("should not throw exception " + e.getMessage());
        }

    }

    @Test
    public void testRunAtDriverContainerDontExist() {

        properties.container.setValue("container-name-ok");
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        existContainer.azureDlsGen2BlobService = blobService;
        try {
            when(blobService.containerExist(anyString())).thenReturn(false);
            existContainer.runAtDriver(runtimeContainer);
        } catch (BlobStorageException e) {
            fail("should not throw exception " + e.getMessage());
        }
    }

    @Test
    public void testrunAtDriverValid() {
        properties.container.setValue("container");
        ValidationResult validationResult = existContainer.initialize(runtimeContainer, properties);
        assertEquals(ValidationResult.OK.getStatus(), validationResult.getStatus());
        existContainer.azureDlsGen2BlobService = blobService;
        try {
            when(blobService.containerExist(anyString())).thenReturn(true);
            existContainer.runAtDriver(runtimeContainer);
        } catch (BlobStorageException e) {
            fail("should not throw exception " + e.getMessage());
        }

    }
}
