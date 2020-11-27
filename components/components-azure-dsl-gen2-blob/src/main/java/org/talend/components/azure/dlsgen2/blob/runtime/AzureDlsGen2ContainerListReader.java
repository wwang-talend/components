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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobStorageException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListProperties;

public class AzureDlsGen2ContainerListReader extends AzureDlsGen2Reader<IndexedRecord> {

    private IndexedRecord currentRecord;

    private TAzureDlsGen2ContainerListProperties properties;

    private transient Iterator<BlobContainerItem> containers;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ContainerListReader.class);

    private Boolean startable = null; // this is initialized in the start method

    Boolean advanceable = null; // this is initialized in the advance method

    /**
     * let this attribute public for test purpose
     */
    public AzureDlsGen2BlobService blobService;

    public AzureDlsGen2ContainerListReader(RuntimeContainer container, BoundedSource source,
                                           TAzureDlsGen2ContainerListProperties properties) {
        super(container, source);
        this.properties = properties;
        AzureDlsGen2Source currentSource = (AzureDlsGen2Source) source;
        this.blobService = new AzureDlsGen2BlobService(currentSource.getAzureConnection(container));
    }

    @Override
    public boolean start() throws IOException {
        startable = false;
        try {
            containers = blobService.listContainers().iterator();
            startable = containers.hasNext();
        } catch (BlobStorageException e) {
            LOGGER.error(e.getLocalizedMessage());
            if (properties.dieOnError.getValue()) {
                throw new ComponentException(e);
            } else {
                startable = false;
            }
        }
        if (startable) {
            dataCount++;
            currentRecord = new GenericData.Record(properties.schema.schema.getValue());
            currentRecord.put(0, containers.next().getName());
        }
        return startable;
    }

    @Override
    public boolean advance() throws IOException {
        advanceable = containers.hasNext();
        if (advanceable) {
            dataCount++;
            currentRecord = new GenericData.Record(properties.schema.schema.getValue());
            currentRecord.put(0, containers.next().getName());
        }
        return advanceable;
    }

    @Override
    public IndexedRecord getCurrent() throws NoSuchElementException {
        if (startable == null || (advanceable != null && !advanceable)) {
            throw new NoSuchElementException();
        }

        return currentRecord;
    }

    @Override
    public Map<String, Object> getReturnValues() {
        Map<String, Object> resultMap = super.getReturnValues();
        resultMap.put(TAzureDlsGen2ContainerListDefinition.RETURN_TOTAL_RECORD_COUNT, dataCount);

        return resultMap;
    }
}
