package org.ohnlp.cat.evidence;

import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.ResourceProvider;
import org.ohnlp.cat.common.impl.ehr.OHDSICDMResourceProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class EvidenceProvider {

    private Map<String, ResourceProvider> resourceProviders;
    private Map<String, Map<ClinicalEntityType, String>> resourceQueries;
    private Map<String, Connection> connections;
    private FhirContext context = FhirContext.forR4Cached();
    private ThreadLocal<ObjectMapper> om = ThreadLocal.withInitial(ObjectMapper::new);

    @Autowired
    public EvidenceProvider(ApplicationConfiguration configuration) {
        resourceProviders = new HashMap<>();
        resourceQueries = new HashMap<>();
        connections = new HashMap<>();
        configuration.getEvidenceProviders().forEach((name, settings) -> {
            ApplicationConfiguration.EvidenceProviderConfig.ProviderConfig providerConfig = settings.getProvider();
            ApplicationConfiguration.EvidenceProviderConfig.ConnectionConfig connConfig = settings.getConnection();
            try {
                ResourceProvider provider = (ResourceProvider) Class.forName(providerConfig.getClazz()).getDeclaredConstructor().newInstance();
                provider.init(providerConfig.getConfig());
                resourceProviders.put(name, provider);
                resourceQueries.put(name, initResourceQueries(provider));

                Class.forName(connConfig.getDriverClass());
                connections.put(name, DriverManager.getConnection(connConfig.getUrl())); // todo handle more config settings
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException | ClassNotFoundException | SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Map<ClinicalEntityType, String> initResourceQueries(ResourceProvider resourceProvider) {
        Map<ClinicalEntityType, String> ret = new HashMap<>();
        for (ClinicalEntityType type : ClinicalEntityType.values()) {
            ret.put(type, String.join(" ",
                    "SELECT * FROM (" + resourceProvider.getQuery(type) + ") cat_resource_query",
                    "WHERE",
                    resourceProvider.getEvidenceIDFilter(type))
            );
        }
        return ret;
    }

    public JsonNode getEvidenceForUID(String evidenceUID) {
        // First parse out actual evidence ID/type
        String id = evidenceUID;
        String providerID = "ehr";
        if (id.startsWith("nlp:")) { // TODO always delineate source provider ID/not always NLP
            id = id.substring(4);
            providerID = "nlp";
        }
        ClinicalEntityType type = ClinicalEntityType.valueOf(id.substring(0, id.indexOf(":")));
        id = id.substring(id.indexOf(":") + 1);
        String basePS;
        Object[] params;
        ResourceProvider provider = resourceProviders.get(providerID);
        Map<ClinicalEntityType, String> queries = resourceQueries.get(providerID);
        Connection conn = connections.get(providerID);
        basePS = queries.get(type);
        params = provider.parseIDTagToParams(type, id);
        try {
            PreparedStatement ps = conn.prepareStatement(basePS);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                // Map to Beam Row Schema
                Schema beamSchema = provider.getQuerySchema(type);
                List<Object> values = new ArrayList<>();
                for (String fieldName : beamSchema.getFieldNames()) {
                    Object val = rs.getObject(fieldName);
                    if (beamSchema.getField(fieldName).getType().getTypeName().isDateType()) {
                        values.add(DateTime.parse(val.toString()));
                    } else {
                        values.add(rs.getObject(fieldName));
                    }
                }
                Row r = Row.withSchema(beamSchema).addValues(values).build();
                // And now call the appropriate mapping function
                return om.get().readTree(context.newJsonParser().encodeResourceToString(provider.getRowToResourceMapper(type).apply(r)));
            }
        } catch (SQLException e) {
            e.printStackTrace(); // TODO
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
