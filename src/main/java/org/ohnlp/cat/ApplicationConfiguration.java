package org.ohnlp.cat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix="cat")
public class ApplicationConfiguration {

    private String applicationURL;

    public String getApplicationURL() {
        return applicationURL;
    }

    public void setApplicationURL(String applicationURL) {
        this.applicationURL = applicationURL;
    }

    private Persistence persistence;

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
    }

    private String jobExecutorClass;

    public String getJobExecutorClass() {
        return jobExecutorClass;
    }

    public void setJobExecutorClass(String jobExecutorClass) {
        this.jobExecutorClass = jobExecutorClass;
    }

    private LDAPConfig ldap;

    public LDAPConfig getLdap() {
        return ldap;
    }

    public void setLdap(LDAPConfig ldap) {
        this.ldap = ldap;
    }

    public static class Persistence {
        private String url;
        private String user;
        private String pwd;
        private String driverClass;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPwd() {
            return pwd;
        }

        public void setPwd(String pwd) {
            this.pwd = pwd;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public void setDriverClass(String driverClass) {
            this.driverClass = driverClass;
        }
    }

    public static class LDAPConfig {
        private boolean enabled;
        private String ldapURL;
        @Value("#{'${cat.ldap.bind-patterns}'.split('|')}")
        private List<String> bindPatterns;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getLdapURL() {
            return ldapURL;
        }

        public void setLdapURL(String ldapURL) {
            this.ldapURL = ldapURL;
        }

        public List<String> getBindPatterns() {
            return bindPatterns;
        }

        public void setBindPatterns(List<String> bindPatterns) {
            this.bindPatterns = bindPatterns;
        }
    }
}
