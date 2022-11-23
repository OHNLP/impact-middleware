package org.ohnlp.cat;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.userdetails.InetOrgPerson;
import org.springframework.security.ldap.userdetails.LdapUserDetailsMapper;
import org.springframework.security.ldap.userdetails.UserDetailsContextMapper;

import java.util.Collection;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

    private final ApplicationConfiguration config;

    public WebSecurityConfig(ApplicationConfiguration config) {
        this.config = config;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        if (config.getLdap().isEnabled()) {
            http
                    .authorizeRequests()
                    .anyRequest().fullyAuthenticated()
                    .and()
                    .httpBasic()
                    .and()
                    .cors()
                    .and()
                    .csrf().disable();
        }
    }

    @Override
    public void configure(AuthenticationManagerBuilder auth) throws Exception {
        DefaultSpringSecurityContextSource contextSource =
                new DefaultSpringSecurityContextSource(config.getLdap().getLdapURL());
        try {
            contextSource.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        BindAuthenticator bind = new BindAuthenticator(contextSource);
        bind.setUserDnPatterns(config.getLdap().getBindPatterns().toArray(new String[0]));
        bind.afterPropertiesSet();
        LdapAuthenticationProvider provider = new LdapAuthenticationProvider(bind);
        provider.setUserDetailsContextMapper(new LdapUserDetailsMapper());
        auth.authenticationProvider(provider);
    }

}
