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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2BlobProperties;
import org.talend.components.azure.dlsgen2.blob.tazurestorageget.TAzureDlsGen2GetProperties;
import org.talend.components.azure.dlsgen2.blob.tazurestoragelist.TAzureDlsGen2ListProperties;

public class AzureDlsGen2SourceTest {

    AzureDlsGen2Source source;

    @Before
    public void setUp() throws Exception {
        source = new AzureDlsGen2Source();
        AzureDlsGen2BlobProperties props = new TAzureDlsGen2ListProperties("test");
    }

    /**
     * Test method for
     * {@link AzureDlsGen2Source#validate(org.talend.components.api.container.RuntimeContainer)}.
     */
    @Test
    public final void testValidate() {

        // assertEquals(ValidationResult.Result.OK, source.validate(null));

    }

    /**
     * Test method for {@link AzureDlsGen2Source#getRemoteBlobs()}.
     */
    @Test
    public final void testGetRemoteBlobs() {
        TAzureDlsGen2GetProperties props = new TAzureDlsGen2GetProperties("test");
        props.setupProperties();
        List<String> prefixes = Arrays.asList("test1", "test2");
        List<Boolean> includes = Arrays.asList(true, false);
        props.remoteBlobs.prefix.setValue(prefixes);
        props.remoteBlobs.include.setValue(includes);

        source.initialize(null, props);
        assertNotNull(source.getRemoteBlobs());

    }

    /**
     * Test method for
     * {@link AzureDlsGen2Source#splitIntoBundles(long, org.talend.components.api.container.RuntimeContainer)}.
     *
     * @throws Exception
     */
    @Test
    public final void testSplitIntoBundles() throws Exception {
        assertTrue(source.splitIntoBundles(0, null).get(0) instanceof AzureDlsGen2Source);
    }

    /**
     * Test method for
     * {@link AzureDlsGen2Source#getEstimatedSizeBytes(org.talend.components.api.container.RuntimeContainer)}.
     */
    @Test
    public final void testGetEstimatedSizeBytes() {
        assertEquals(0, source.getEstimatedSizeBytes(null));
    }

    /**
     * Test method for
     * {@link AzureDlsGen2Source#producesSortedKeys(org.talend.components.api.container.RuntimeContainer)}.
     */
    @Test
    public final void testProducesSortedKeys() {
        assertFalse(source.producesSortedKeys(null));
    }

}
