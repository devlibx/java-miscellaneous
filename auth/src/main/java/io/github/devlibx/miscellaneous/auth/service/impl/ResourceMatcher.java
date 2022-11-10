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

    private boolean internalMatch(String requested, String available) {
        verifyValidResourceType(requested);
        verifyValidResourceType(available);
        return Objects.equals(requested, available);
    }

    @Override
    public boolean match(List<String> requested, List<String> available) {
        if (available != null) {
            for (String r : available) {
                for (String r1 : requested) {
                    if (internalMatch(r1, r)) {
                        return true;
                    }
                }
            }
        }
        return false;
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
