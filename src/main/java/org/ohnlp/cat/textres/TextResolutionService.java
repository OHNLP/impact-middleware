package org.ohnlp.cat.textres;

import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.parser.DataSourceRepresentation;
import org.ohnlp.cat.api.criteria.parser.UMLSDataSourceRepresentationResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class TextResolutionService {
    private final ApplicationConfiguration config;
    private final ThreadLocal<NLPPipeline> nlp;
    // Use a fixed thread pool to ensure limited number of NLP Pipelines created from the ThreadLocal instead of one per request thread
    private final ExecutorService nlpExecutor = Executors.newFixedThreadPool(2); // TODO make this configurable

    private final UMLSDataSourceResolverProvider representationResolvers;

    @Autowired
    public TextResolutionService(ApplicationConfiguration config, UMLSDataSourceResolverProvider representationResolvers) {
        this.config = config;
        this.nlp = ThreadLocal.withInitial(() -> new NLPPipeline(this.config));
        this.representationResolvers = representationResolvers;
    }

    private Set<String> parseToUMLS(String in) {
        try {
            return nlpExecutor.submit(() -> nlp.get().process(in)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get UMLS CUIs", e);
        }
    }

    public Collection<DataSourceRepresentation> getDataSourceRepresentations(String in, ClinicalEntityType type, List<String> dataSourceIDs) {
        Set<String> umls = parseToUMLS(in);
        HashSet<DataSourceRepresentation> ret = new HashSet<>();
        for (String dataSourceID : dataSourceIDs) {
            for (String cui : umls) {
                Map<String, UMLSDataSourceRepresentationResolver> representations = this.representationResolvers.getResolversForDataSource(dataSourceID);
                representations.forEach((name, resolver) -> {
                    ret.addAll(resolver.resolveForUMLS(type, dataSourceID, cui));
                });
            }
        }
        return ret;
    }
}
