package io.github.devlibx.miscellaneous.auth.service;

public interface IResourceMatcher {
    boolean match(String requested, String available);
}
