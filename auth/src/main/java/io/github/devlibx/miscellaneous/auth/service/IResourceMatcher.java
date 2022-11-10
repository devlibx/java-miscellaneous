package io.github.devlibx.miscellaneous.auth.service;

import java.util.List;

public interface IResourceMatcher {
    boolean match(String requested, String available);

    boolean match(String requested, List<String> available);
}
