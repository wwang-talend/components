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
package org.talend.components.azure.dlsgen2.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;

public class AzureDlsGen2UtilsTest {

    private static final I18nMessages i18nMessages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2Utils.class);

    private AzureDlsGen2Utils azureDlsGen2Utils;

    String remotedir = "remote-azure";

    String localdir = "azure";

    String keyparent = "parent";

    String folder;

    String TEST_FOLDER_PUT = "azurestorage-put";

    @Before
    public void setUp() throws Exception {
        azureDlsGen2Utils = new AzureDlsGen2Utils();
        folder = getClass().getClassLoader().getResource("azurestorage-put").getPath();
    }

    /**
     * @see AzureDlsGen2Utils#genAzureObjectList(File, String)
     */
    @Test
    public void testGenAzureObjectList() {
        File file = new File(folder);
        Map<String, String> result = azureDlsGen2Utils.genAzureObjectList(file, keyparent);
        assertNotNull("result cannot be null", result);

    }

    @Test
    public void testIlleagueArguementException() {
        try {
            File file = new File(folder + "/blob1.txt");
            azureDlsGen2Utils.genAzureObjectList(file, keyparent);
        } catch (IllegalArgumentException ilae) {
            assertEquals(i18nMessages.getMessage("error.invalidDirectory"), ilae.getMessage());
        }
    }

    /**
     * @see AzureDlsGen2Utils#genFileFilterList(List<Map<String,String>>,String,String)
     */
    @Test
    public void genFileFilterList() {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        Map myMap = new HashMap<String, String>();
        myMap.put("*.txt", "b");
        myMap.put("*", "d");
        myMap.put("c", "d");
        list.add(myMap);
        Map<String, String> result = azureDlsGen2Utils.genFileFilterList(list, folder, remotedir);
        assertNotNull("result cannot be null", result);
    }

}
