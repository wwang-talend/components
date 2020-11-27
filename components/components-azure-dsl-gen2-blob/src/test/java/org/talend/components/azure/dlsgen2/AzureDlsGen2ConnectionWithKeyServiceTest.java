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
package org.talend.components.azure.dlsgen2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class AzureDlsGen2ConnectionWithKeyServiceTest {

    @Test
    public void testBuildInsatnce() {
        AzureDlsGen2ConnectionWithKeyService instance = AzureDlsGen2ConnectionWithKeyService.builder()//
                .protocol("http")//
                .accountName("talendAccount")//
                .accountKey("aValide64baseKey")//
                .build();

        assertNotNull(instance);
        assertEquals("http", instance.getProtocol());
        assertEquals("talendAccount", instance.getAccountName());
        assertEquals("aValide64baseKey", instance.getAccountKey());
    }
}
