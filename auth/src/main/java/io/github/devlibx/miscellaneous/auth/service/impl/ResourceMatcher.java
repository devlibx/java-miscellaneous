package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.service.IResourceMatcher;

import java.util.Objects;

public class ResourceMatcher implements IResourceMatcher {

    @Override
    public boolean match(String requested, String available) {
        if (Objects.equals(requested, available)) {
            return true;
        }
        return false;
    }
}
