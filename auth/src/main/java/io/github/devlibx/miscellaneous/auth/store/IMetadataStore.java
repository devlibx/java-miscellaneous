package io.github.devlibx.miscellaneous.auth.store;

import java.util.List;

public interface IMetadataStore {
    List<String> getResourceTypes();
    List<String> getServiceTypes();
}
