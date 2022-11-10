package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.exception.ActionNowAllowedOnResourceException;
import io.github.devlibx.miscellaneous.auth.pojo.Action;
import io.github.devlibx.miscellaneous.auth.pojo.Effect;
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

                // Check for allow action first
                if (Objects.equals(statement.getEffect(), Effect.Allow)) {

                    // Make sure the resource in a requested action matches this statement rule
                    if (resourceMatcher.match(action.getResource(), statement.getResource())
                            || resourceMatcher.match(action.getResource(), statement.getResources())
                    ) {

                        // Check if this action is allowed
                        for (String actionFromPolicy : statement.getActions()) {
                            if (policyActionMatcher.match(action.getAction(), actionFromPolicy)) {
                                allowed = true;
                            }
                        }

                        // Check if this action is not allowed - if it was allowed by any action, but any non-action will
                        // suppress it
                        for (String actionFromPolicy : statement.getNotActions()) {
                            if (policyActionMatcher.match(action.getAction(), actionFromPolicy)) {
                                allowed = false;
                            }
                        }
                    }
                }
            }
        }

        // Throw auth error if action and policy does not match
        if (!allowed) {
            throw new ActionNowAllowedOnResourceException(action);
        }
    }
}
