package io.github.devlibx.miscellaneous.auth.module;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.github.devlibx.miscellaneous.auth.service.IPolicyActionMatcher;
import io.github.devlibx.miscellaneous.auth.service.IPolicyValidator;
import io.github.devlibx.miscellaneous.auth.service.IResourceMatcher;
import io.github.devlibx.miscellaneous.auth.service.impl.BasicPolicyValidator;
import io.github.devlibx.miscellaneous.auth.service.impl.PolicyActionMatcher;
import io.github.devlibx.miscellaneous.auth.service.impl.ResourceMatcher;
import io.github.devlibx.miscellaneous.auth.store.IMetadataStore;
import io.github.devlibx.miscellaneous.auth.store.impl.MetadataStore;

public class AuthModule extends AbstractModule {
    @Override
    protected void configure() {
        super.configure();
        bind(IResourceMatcher.class).to(ResourceMatcher.class);
        bind(IPolicyActionMatcher.class).to(PolicyActionMatcher.class);
        bind(IPolicyValidator.class).to(BasicPolicyValidator.class);
        bind(IMetadataStore.class).to(MetadataStore.class).in(Scopes.SINGLETON);
    }
}
