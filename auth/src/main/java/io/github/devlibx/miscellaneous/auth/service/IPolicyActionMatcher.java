package io.github.devlibx.miscellaneous.auth.service;

public interface IPolicyActionMatcher {
    boolean match(String requested, String available);
}
