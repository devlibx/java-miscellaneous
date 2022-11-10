package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.service.IResourceMatcher;

import java.util.List;
import java.util.Objects;

public class ResourceMatcher implements IResourceMatcher {

    @Override
    public boolean match(String requested, String available) {
        if (Objects.equals(requested, available)) {
            return true;
        }
        return false;
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
}
