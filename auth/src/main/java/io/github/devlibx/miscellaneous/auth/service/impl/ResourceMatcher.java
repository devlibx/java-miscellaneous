package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.exception.InvalidResourceTypeException;
import io.github.devlibx.miscellaneous.auth.service.IResourceMatcher;
import io.github.devlibx.miscellaneous.auth.store.IMetadataStore;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;

public class ResourceMatcher implements IResourceMatcher {
    private final IMetadataStore metadataStore;

    @Inject
    public ResourceMatcher(IMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public boolean match(String requested, String available) {
        verifyValidResourceType(requested);
        verifyValidResourceType(available);
        return Objects.equals(requested, available);
    }

    @Override
    public boolean match(String requested, List<String> available) {
        boolean matched = false;
        if (available != null) {
            for (String r : available) {
                if (match(requested, r)) {
                    matched = true;
                    break;
                }
            }
        }
        return matched;
    }

    /**
     * Parse a valid valid resource - sample = arn:org:<This shoule be a valid value></This>:*:*:user_table"
     */
    private void verifyValidResourceType(String value) {

        // If it is "*" then no need to check
        if (Objects.equals(value, "*")) {
            return;
        }

        // We need to split by ":" and look at 2nd token
        String[] tokens = value.split(":");
        if (tokens.length < 2) {
            throw new RuntimeException("Invalid resource value: format should be <resource_type>:.... Provided=" + value);
        }

        // Make sure the resource types is a valid type based on the metadata store
        if (!metadataStore.getResourceTypes().contains(tokens[2])) {
            throw new InvalidResourceTypeException("Invalid resource value: resource is not valid. valid resources=" + metadataStore.getResourceTypes() + "  Provided=" + value);
        }
    }
}
