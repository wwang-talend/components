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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobDefinition;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobService;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerDefinition;
import org.talend.components.azure.dlsgen2.blob.helpers.RemoteBlob;
import org.talend.components.azure.dlsgen2.blob.tazurestoragecontainerlist.TAzureDlsGen2ContainerListDefinition;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListProperties;
import org.talend.components.common.avro.RootSchemaUtils;

public class AzureDlsGen2ListReader extends AzureDlsGen2Reader<IndexedRecord> {

    private TAzureDlsGen2ListProperties properties;

    private Iterator<BlobItem> blobsIterator;

    private BlobItem currentBlob;

    private IndexedRecord currentRecord;

    private boolean startable;

    private Boolean advanceable;

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDlsGen2ListReader.class);

    /**
     * keep this attribute public for test purpose
     */
    public AzureDlsGen2BlobService azureDlsGen2BlobService;

    public AzureDlsGen2ListReader(RuntimeContainer container, BoundedSource source, TAzureDlsGen2ListProperties properties) {
        super(container, source);
        this.properties = properties;
        azureDlsGen2BlobService = new AzureDlsGen2BlobService(
                ((AzureDlsGen2Source) getCurrentSource()).getAzureConnection(container));
    }

    @Override
    public boolean start() throws IOException {
        String mycontainer = properties.container.getValue();
        List<BlobItem> blobs = new ArrayList<>();
        // build a list with remote blobs to fetch
        List<RemoteBlob> remoteBlobs = ((AzureDlsGen2Source) getCurrentSource()).getRemoteBlobs();
        try {

            for (RemoteBlob rmtb : remoteBlobs) {
                for (BlobItem blob : azureDlsGen2BlobService.listBlobs(mycontainer, rmtb.prefix, rmtb.include)) {

                    //if (!Optional.ofNullable(blob.isPrefix()).orElse(false)) {
                    if (AzureDlsGen2ContainerRuntime
                            .isBlobSelectable(rmtb.include, rmtb.prefix, blob.getName()) && !AzureDlsGen2ContainerRuntime
                            .isDirectoryBlob(blob)) {
                        blobs.add(blob);
                    }
                }
            }

            startable = !blobs.isEmpty();
            blobsIterator = blobs.iterator();
        } catch (BlobStorageException e) {
            LOGGER.error(e.getLocalizedMessage());
            if (properties.dieOnError.getValue()) {
                throw new ComponentException(e);
            }
        }

        if (startable) {
            dataCount++;
            currentBlob = blobsIterator.next();
            IndexedRecord dataRecord = new GenericData.Record(properties.schema.schema.getValue());
            dataRecord.put(0, currentBlob.getName());
            Schema rootSchema = RootSchemaUtils
                    .createRootSchema(properties.schema.schema.getValue(), properties.outOfBandSchema);
            currentRecord = new GenericData.Record(rootSchema);
            currentRecord.put(0, dataRecord);
            currentRecord.put(1, dataRecord);
        }
        return startable;
    }

    @Override
    public boolean advance() throws IOException {
        advanceable = blobsIterator.hasNext();
        if (advanceable) {
            dataCount++;
            currentBlob = blobsIterator.next();
            IndexedRecord dataRecord = new GenericData.Record(properties.schema.schema.getValue());
            dataRecord.put(0, currentBlob.getName());
            currentRecord.put(0, dataRecord);
            currentRecord.put(1, dataRecord);
        }

        return advanceable;
    }

    @Override
    public IndexedRecord getCurrent() {
        if (!startable || (advanceable != null && !advanceable)) {
            throw new NoSuchElementException();
        }

        if (runtime != null) {
            runtime.setComponentData(runtime.getCurrentComponentId(), AzureDlsGen2BlobDefinition.RETURN_CURRENT_BLOB,
                                     currentRecord.get(0));
        }

        return currentRecord;
    }

    @Override
    public Map<String, Object> getReturnValues() {
        Map<String, Object> resultMap = super.getReturnValues();
        resultMap.put(TAzureDlsGen2ContainerListDefinition.RETURN_TOTAL_RECORD_COUNT, dataCount);
        resultMap.put(AzureDlsGen2ContainerDefinition.RETURN_CONTAINER, properties.container.getValue());
        if (currentBlob != null) {
            resultMap.put(AzureDlsGen2BlobDefinition.RETURN_CURRENT_BLOB, currentBlob.getName());
        }

        return resultMap;
    }
}
