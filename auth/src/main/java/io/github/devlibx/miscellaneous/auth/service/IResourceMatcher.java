package io.github.devlibx.miscellaneous.auth.service;

import java.util.List;

public interface IResourceMatcher {
    boolean match(List<String> requested, List<String> available);
}
