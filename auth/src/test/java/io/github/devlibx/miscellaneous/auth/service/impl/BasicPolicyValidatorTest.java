package io.github.devlibx.miscellaneous.auth.service.impl;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.miscellaneous.auth.exception.ActionNowAllowedOnResourceException;
import io.github.devlibx.miscellaneous.auth.module.AuthModule;
import io.github.devlibx.miscellaneous.auth.pojo.Action;
import io.github.devlibx.miscellaneous.auth.pojo.Policy;
import io.github.devlibx.miscellaneous.auth.pojo.Statement;
import io.github.devlibx.miscellaneous.auth.service.IPolicyValidator;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;

public class BasicPolicyValidatorTest {
    private IPolicyValidator policyValidator;

    @Before
    public void setup() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
            }
        }, new AuthModule());
        policyValidator = injector.getInstance(IPolicyValidator.class);
    }

    @Test
    public void testAllowPolicy_WhenActionIsAllowedOnResource() {
        // Sample AWS resource
        // "Resource": "arn:aws:sqs:us-east-2:account-ID-without-hyphens:queue1"

        // arn:<org>:<resource type>:<region>:<account>:<resource name>
        String resource = "arn:org:db:*:*:user_table";

        Policy policy = Policy.builder()
                .statements(Collections.singletonList(
                        Statement.builder()
                                .actions(Collections.singletonList(
                                        "db:Get"
                                ))
                                .resource(resource)
                                .build()
                ))
                .build();
        policyValidator.validate(
                Action.builder()
                        .action("db:Get")
                        .resource(resource)
                        .build(),
                Collections.singletonList(policy)
        );
    }

    @Test
    public void testAllowPolicy_WhenActionIsAllowedButResourceDoesNotMatch() {
        String resource = "arn:org:db:*:*:user_table";
        String resourceWhichUserAsked = "arn:org:db:*:*:user_table_dont_allow";

        Policy policy = Policy.builder()
                .statements(Collections.singletonList(
                        Statement.builder()
                                .actions(Collections.singletonList(
                                        "db:Get"
                                ))
                                .resource(resource)
                                .build()
                ))
                .build();
        System.out.println(JsonUtils.asJson(policy));
        Assertions.assertThrowsExactly(
                ActionNowAllowedOnResourceException.class,
                () -> {
                    policyValidator.validate(
                            Action.builder()
                                    .action("db:Get")
                                    .resource(resourceWhichUserAsked)
                                    .build(),
                            Collections.singletonList(policy)
                    );
                }
        );
    }

    /**
     * Here a db:Get was allowed, but it was suppressed by a not-action
     */
    @Test
    public void testAllowPolicy_WhenNonActionWillStopAllowedAction() {
        String resource = "arn:org:db:*:*:user_table";

        Policy policy = Policy.builder()
                .statements(Collections.singletonList(
                        Statement.builder()
                                .actions(Collections.singletonList(
                                        "db:Get"
                                ))
                                .notActions(Collections.singletonList("db:Get"))
                                .resource(resource)
                                .build()
                ))
                .build();

        Assertions.assertThrowsExactly(
                ActionNowAllowedOnResourceException.class,
                () -> {
                    policyValidator.validate(
                            Action.builder()
                                    .action("db:Get")
                                    .resource(resource)
                                    .build(),
                            Collections.singletonList(policy)
                    );
                }
        );
    }

}