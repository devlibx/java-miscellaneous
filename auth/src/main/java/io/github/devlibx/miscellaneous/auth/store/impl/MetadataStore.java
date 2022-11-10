package io.github.devlibx.miscellaneous.auth.store.impl;

import io.github.devlibx.miscellaneous.auth.store.IMetadataStore;

import java.util.Arrays;
import java.util.List;

public class MetadataStore implements IMetadataStore {

    @Override
    public List<String> getResourceTypes() {
        return Arrays.asList(
                "db",
                "node"
        );
    }

    @Override
    public List<String> getServiceTypes() {
        return Arrays.asList(
                "db",
                "node"
        );
    }
}
