package org.ohnlp.cat.textres;

import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.api.criteria.parser.UMLSDataSourceRepresentationResolver;
import org.ohnlp.cat.api.ehr.DataSourceInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class UMLSDataSourceResolverProvider {
    private final ApplicationConfiguration config;
    private Map<String, List<String>> dataSourceToRepresentationResolverID;
    private Map<String, UMLSDataSourceRepresentationResolver> representationResolvers;

    @Autowired
    public UMLSDataSourceResolverProvider(ApplicationConfiguration config) {
        this.config = config;
    }

    private void init(ApplicationConfiguration config) {
        this.dataSourceToRepresentationResolverID = new HashMap<>();
        this.representationResolvers = new HashMap<>();
        for (ApplicationConfiguration.RepresentationResolverConfig resolverConfig : config.getRepresentationResolvers()) {
            String id = resolverConfig.getId();
            String clazz = resolverConfig.getResolverClass();
            try {
                UMLSDataSourceRepresentationResolver resolver =
                        (UMLSDataSourceRepresentationResolver) Class.forName(clazz).getDeclaredConstructor().newInstance();
                resolver.init(resolverConfig.getConfig());
                representationResolvers.put(id, resolver);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
                throw new IllegalArgumentException("Failed to instantiate representation resolver", e);
            }
        }
        for (DataSourceInformation dataSource : config.getDataSources()) {
            dataSourceToRepresentationResolverID.put(dataSource.getBackendID(), dataSource.getTextResolvers());
        }
    }

    public Map<String, UMLSDataSourceRepresentationResolver> getResolversForDataSource(String datasource) {
        Map<String, UMLSDataSourceRepresentationResolver> ret = new HashMap<>();
        for (String resolverID : dataSourceToRepresentationResolverID.get(datasource)) {
            ret.put(resolverID, representationResolvers.get(resolverID));
        }
        return ret;
    }

}
