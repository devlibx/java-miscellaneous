package io.github.devlibx.miscellaneous.auth.service;

import io.github.devlibx.miscellaneous.auth.pojo.Action;
import io.github.devlibx.miscellaneous.auth.pojo.Policy;

import java.util.List;

public interface IPolicyValidator {
    void validate(Action action, List<Policy> policies);
}
