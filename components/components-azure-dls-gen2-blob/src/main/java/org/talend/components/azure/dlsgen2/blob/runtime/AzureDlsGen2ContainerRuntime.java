// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.azure.storage.blob.models.BlobItem;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azure.dlsgen2.blob.AzureDlsGen2ContainerProperties;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

public class AzureDlsGen2ContainerRuntime extends AzureDlsGen2Runtime {

    private static final long serialVersionUID = 312081420617929183L;

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(AzureDlsGen2ContainerCreateRuntime.class);

    protected String containerName;

    protected boolean dieOnError;

    public static boolean isBlobSelectable(final boolean recursiveSelection, final String prefix, final String blob) {
        return recursiveSelection || (!recursiveSelection && blob.replace(prefix, "").replaceAll("^/", "")
                .indexOf("/") == -1);
    }

    public static boolean isDirectoryBlob(final BlobItem blob) {
        final Map<String, String> meta = Optional.ofNullable(blob.getMetadata()).orElse(Collections.emptyMap());
        return Boolean.valueOf(meta.getOrDefault("hdi_isfolder", "false"));
    }

    @Override
    public ValidationResult initialize(RuntimeContainer runtimeContainer, ComponentProperties properties) {
        // init
        AzureDlsGen2ContainerProperties componentProperties = (AzureDlsGen2ContainerProperties) properties;

        this.containerName = componentProperties.container.getValue();

        // validate
        ValidationResult validationResult = super.initialize(runtimeContainer, properties);
        if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
            return validationResult;
        }

        String errorMessage = "";
        // not empty
        if (StringUtils.isEmpty(containerName)) {
            errorMessage = messages.getMessage("error.ContainerEmpty");
        }
        // valid characters 0-9 a-z and -
        else if (!StringUtils.isAlphanumeric(containerName.replaceAll("-", ""))) {

            errorMessage = messages.getMessage("error.IncorrectName");
        }
        // all lowercase
        else if (!StringUtils.isAllLowerCase(containerName.replaceAll("(-|\\d)", ""))) {
            errorMessage = messages.getMessage("error.UppercaseName");
        }
        // length range : 3-63
        else if ((containerName.length() < 3) || (containerName.length() > 63)) {
            errorMessage = messages.getMessage("error.LengthError");
        }

        if (errorMessage.isEmpty()) {
            return ValidationResult.OK;
        } else {
            return new ValidationResult(ValidationResult.Result.ERROR, errorMessage);
        }
    }

}
