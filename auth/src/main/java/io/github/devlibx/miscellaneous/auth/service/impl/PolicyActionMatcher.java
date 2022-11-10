package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.service.IPolicyActionMatcher;

import java.util.Objects;

public class PolicyActionMatcher implements IPolicyActionMatcher {
    @Override
    public boolean match(String requested, String available) {
        if (Objects.equals(requested, available)) {
            return true;
        }
        return false;
    }
}
