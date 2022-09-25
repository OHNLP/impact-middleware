package org.ohnlp.cat.evidence;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.ResourceProvider;
import org.ohnlp.cat.common.impl.ehr.OHDSICDMResourceProvider;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class EvidenceProvider {
    private ResourceProvider ehrResourceProvider = new OHDSICDMResourceProvider(); // TODO Autowire
    private ResourceProvider nlpResourceProvider = new OHDSICDMResourceProvider(); // TODO Autowire
    private Map<ClinicalEntityType, String> ehrResourceQueries = initResourceQueries(ehrResourceProvider);
    private Map<ClinicalEntityType, String> nlpResourceQueries = initResourceQueries(nlpResourceProvider);
    private Connection ehrConn; // TODO
    private Connection nlpConn; // TODO

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

    public DomainResource getEvidenceForUID(String evidenceUID) {
        // First parse out actual evidence ID/type
        String id = evidenceUID;
        boolean nlp = false;
        if (id.startsWith("nlp:")) {
            nlp = true;
            id = id.substring(4);
        }
        ClinicalEntityType type = ClinicalEntityType.valueOf(id.substring(0, id.indexOf(":")));
        id = id.substring(id.indexOf(":") + 1);
        String basePS;
        Object[] params;
        ResourceProvider provider = nlp ? nlpResourceProvider : ehrResourceProvider;
        Map<ClinicalEntityType, String> queries = nlp ? nlpResourceQueries : ehrResourceQueries;
        Connection conn = nlp ? nlpConn : ehrConn;
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
                    values.add(rs.getObject(fieldName)); // TODO might need to handle JODA date conversion here.
                }
                Row r = Row.withSchema(beamSchema).addValues(values).build();
                // And now call the appropriate mapping function
                return provider.getRowToResourceMapper(type).apply(r);
            }
        } catch (SQLException e) {
            e.printStackTrace(); // TODO
        }
        return null;
    }
}
