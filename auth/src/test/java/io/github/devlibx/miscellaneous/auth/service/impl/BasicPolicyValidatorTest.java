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
        Policy policy = Policy.builder()
                .statements(Collections.singletonList(
                        Statement.builder()
                                .actions(Collections.singletonList(
                                        "db:Get"
                                ))
                                .resource("arn:harish:db::user_table")
                                .build()
                ))
                .build();
        policyValidator.validate(
                Action.builder()
                        .action("db:Get")
                        .resource("arn:harish:db::user_table")
                        .build(),
                Collections.singletonList(policy)
        );
    }

    @Test
    public void testAllowPolicy_WhenActionIsAllowedButResourceDoesNotMatch() {
        Policy policy = Policy.builder()
                .statements(Collections.singletonList(
                        Statement.builder()
                                .actions(Collections.singletonList(
                                        "db:Get"
                                ))
                                .resource("arn:harish:db::user_table")
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
                                    .resource("arn:harish:db::user_table_bad_table")
                                    .build(),
                            Collections.singletonList(policy)
                    );
                }
        );
    }
}