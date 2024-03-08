package org.ohnlp.cat;

import org.ohnlp.cat.api.ehr.DataSourceInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix="cat")
public class ApplicationConfiguration {

    private String backendCallbackUsername;

    public String getBackendCallbackUsername() {
        return backendCallbackUsername;
    }

    public void setBackendCallbackUsername(String backendCallbackUsername) {
        this.backendCallbackUsername = backendCallbackUsername;
    }

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

    public Map<String, EvidenceProviderConfig> evidenceProviders;

    public Map<String, EvidenceProviderConfig> getEvidenceProviders() {
        return evidenceProviders;
    }

    public void setEvidenceProviders(Map<String, EvidenceProviderConfig> evidenceProviders) {
        this.evidenceProviders = evidenceProviders;
    }

    public List<DataSourceInformation> dataSources;

    public List<DataSourceInformation> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<DataSourceInformation> dataSources) {
        this.dataSources = dataSources;
    }

    public List<RepresentationResolverConfig> representationResolvers;

    public List<RepresentationResolverConfig> getRepresentationResolvers() {
        return representationResolvers;
    }

    public void setRepresentationResolvers(List<RepresentationResolverConfig> representationResolvers) {
        this.representationResolvers = representationResolvers;
    }

    public static class Persistence {
        private String url;
        private String user;
        private String pwd;
        private String schema;
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

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
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

    public static class EvidenceProviderConfig {
        private ProviderConfig provider;
        private ConnectionConfig connection;

        public ProviderConfig getProvider() {
            return provider;
        }

        public void setProvider(ProviderConfig provider) {
            this.provider = provider;
        }

        public ConnectionConfig getConnection() {
            return connection;
        }

        public void setConnection(ConnectionConfig connection) {
            this.connection = connection;
        }

        public static class ProviderConfig {
            private String clazz;
            private Map<String, Object> config;

            public String getClazz() {
                return clazz;
            }

            public void setClazz(String clazz) {
                this.clazz = clazz;
            }

            public Map<String, Object> getConfig() {
                return config;
            }

            public void setConfig(Map<String, Object> config) {
                this.config = config;
            }
        }

        public static class ConnectionConfig {
            private String url;
            private String driverClass;

            public String getUrl() {
                return url;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            public String getDriverClass() {
                return driverClass;
            }

            public void setDriverClass(String driverClass) {
                this.driverClass = driverClass;
            }
        }
    }

    public static class RepresentationResolverConfig {
        private String id;
        private String resolverClass;
        private Map<String, Object> config;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getResolverClass() {
            return resolverClass;
        }

        public void setResolverClass(String resolverClass) {
            this.resolverClass = resolverClass;
        }

        public Map<String, Object> getConfig() {
            return config;
        }

        public void setConfig(Map<String, Object> config) {
            this.config = config;
        }
    }
}
