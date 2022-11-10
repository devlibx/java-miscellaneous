package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.exception.ActionNowAllowedOnResourceException;
import io.github.devlibx.miscellaneous.auth.pojo.Action;
import io.github.devlibx.miscellaneous.auth.pojo.Policy;
import io.github.devlibx.miscellaneous.auth.pojo.Statement;
import io.github.devlibx.miscellaneous.auth.service.IPolicyValidator;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BasicPolicyValidator implements IPolicyValidator {
    private final ResourceMatcher resourceMatcher;
    private final PolicyActionMatcher policyActionMatcher;

    @Inject
    public BasicPolicyValidator(ResourceMatcher resourceMatcher, PolicyActionMatcher policyActionMatcher) {
        this.resourceMatcher = resourceMatcher;
        this.policyActionMatcher = policyActionMatcher;
    }

    @Override
    public void validate(Action action, List<Policy> policies) {
        boolean allowed = false;
        for (Policy policy : policies.stream().filter(policy -> Objects.equals("v1", policy.getVersion())).collect(Collectors.toList())) {
            for (Statement statement : policy.getStatements()) {
                if (resourceMatcher.match(action.getResource(), statement.getResource())) {
                    for (String actionFromPolicy : statement.getActions()) {
                        if (policyActionMatcher.match(action.getAction(), actionFromPolicy)) {
                            allowed = true;
                        }
                    }
                }
            }
        }
        if (!allowed) {
            throw new ActionNowAllowedOnResourceException(action);
        }
    }
}
