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

import javax.inject.Inject;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.components.azure.dlsgen2.blob.runtime.AzureDlsGen2Source;

/**
 * Class AzureDlsGen2BaseTestIT.
 */
public abstract class AzureDlsGen2BaseTestIT {

    public String TEST_NAME;

    static public String accountKey = System.getProperty("azurestorage.account.key");

    static public String accountName = System.getProperty("azurestorage.account.name");

    static public String sharedAccessSignature = System.getProperty("azurestorage.sharedaccesssignature");

    static public String useSAS = System.getProperty("azurestorage.usesas");

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    @Inject
    private ComponentService componentService;

    protected RuntimeContainer runtime;

    private transient static final Logger LOG = LoggerFactory.getLogger(AzureDlsGen2BaseTestIT.class);

    /**
     * Instantiates a new AzureDlsGen2BaseTestIT().
     */
    public AzureDlsGen2BaseTestIT(String testName) {
        TEST_NAME = testName;
        runtime = new DefaultComponentRuntimeContainerImpl();
    }

    // default implementation for pure java test. Shall be overriden of Spring or OSGI tests
    public ComponentService getComponentService() {
        if (componentService == null) {
            DefinitionRegistry testComponentRegistry = new DefinitionRegistry();
            // register component
            testComponentRegistry.registerComponentFamilyDefinition(new AzureDlsGen2FamilyDefinition());
            componentService = new ComponentServiceImpl(testComponentRegistry);
        }
        return componentService;
    }

    /**
     * initializeComponentRegistryAndService.
     */
    @Before
    public void initializeComponentRegistryAndService() {
        // reset the component service
        componentService = null;
    }

    public static String getRandomTestUID() {
        return RandomStringUtils.randomNumeric(10);
    }

    public String getNamedThingForTest(String aThing) {
        return aThing + TEST_NAME;
    }

    /**
     * createBoundedReader.
     *
     * @param <T>   the generic type
     * @param props {@link ComponentProperties} props
     *
     * @return <code>BoundedReader</code> {@link BoundedReader} bounded reader
     */
    @SuppressWarnings("unchecked")
    public <T> BoundedReader<T> createBoundedReader(ComponentProperties props) {
        AzureDlsGen2Source source = new AzureDlsGen2Source();
        source.initialize(null, props);
        source.validate(null);
        return source.createReader(null);
    }

    /**
     * setupContainerProperties - return Connection properties filled in.
     *
     * @param properties {@link AzureDlsGen2Properties} properties
     *
     * @return <code>AzureStorageProperties</code> {@link AzureDlsGen2Properties} azure storage properties
     */
    public static AzureDlsGen2ProvideConnectionProperties setupConnectionProperties(
            AzureDlsGen2ProvideConnectionProperties properties) {
        properties.getConnectionProperties().setupProperties();
        properties.getConnectionProperties().accountName.setValue(accountName);
        properties.getConnectionProperties().accountKey.setValue(accountKey);
        properties.getConnectionProperties().sharedAccessSignature.setValue(sharedAccessSignature);
        boolean sas = Boolean.parseBoolean(useSAS);
        if (sas) {
            properties.getConnectionProperties().useSharedAccessSignature.setValue(sas);
        }
        return properties;
    }
}
