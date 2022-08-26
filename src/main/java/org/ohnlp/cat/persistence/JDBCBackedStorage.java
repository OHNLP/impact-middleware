package org.ohnlp.cat.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.dto.CohortDefinitionDTO;
import org.ohnlp.cat.dto.PatInfoDTO;
import org.ohnlp.cat.dto.ProjectDTO;
import org.ohnlp.cat.dto.StructuredEvidenceDTO;
import org.ohnlp.cat.dto.enums.CohortInclusion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

// TODO permissions checks for all functions?
@Component
public class JDBCBackedStorage {
    private ComboPooledDataSource datasource;
    private ThreadLocal<ObjectMapper> om = ThreadLocal.withInitial(ObjectMapper::new);

    @Autowired
    public JDBCBackedStorage(ApplicationConfiguration config) {
        initDBConn(config);
    }

    // ===== Project Management Methods ===== //
    public List<ProjectDTO> getProjectList(Authentication authentication) {
        List<ProjectDTO> ret = new ArrayList<>();
        try (Connection conn = this.datasource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM cat.projects p"); // TODO Handle shared project lookups
        } catch (SQLException e) {
            e.printStackTrace();  // TODO log exceptions to db
        }
        return ret;
    }

    // ===== Cohort Related Methods ===== //
    public CohortDefinitionDTO getCohortCriteria(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement("SELECT cohort_def FROM cat.cohorts p WHERE project_uid = ?"); // TODO
            ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return om.get().readValue(rs.getString(1), CohortDefinitionDTO.class);
            } else {
                return null; // No Cohort Definition Yet
            }
        } catch (SQLException | JsonProcessingException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort criteria retrieve", e);
        }
    }

    public void writeCohortCriteria(Authentication authentication, UUID projectUID, CohortDefinitionDTO def) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            // Manually check exists instead of using MERGE because not all SQL dialects support it
            PreparedStatement checkExists = conn.prepareStatement("SELECT cohort_def FROM cat.cohort_defs c WHERE project_uid = ?");
            checkExists.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
            if (checkExists.executeQuery().next()) {
                PreparedStatement ps = conn.prepareStatement("UPDATE cat.cohort_defs c SET cohort_def = ? WHERE project_uid = ?"); // TODO
                ps.setString(1, om.get().writeValueAsString(def));
                ps.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.executeUpdate();
            } else {
                PreparedStatement ps = conn.prepareStatement("INSERT INTO cat.cohort_defs (project_uid, cohort_def) VALUES (?, ?)"); // TODO
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, om.get().writeValueAsString(def));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort criteria write", e);
        }
    }

    // Gets the current evaluated cohort for a given project/user. TODO consider adding pagination in returned results (sort by score)
    public List<PatInfoDTO> getRetrievedCohort(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT c.pat_id, c.pat_name, c.pat_dob, c.pat_birth_gender_male_flag, j.judgement " +
                            "FROM cat.cohorts c LEFT JOIN (SELECT * FROM cat.judgements WHERE judger_id = ?) j" +
                            "     ON c.project_uid = j.project_uid AND c.pat_id = j.pat_id " +
                            "WHERE c.project_uid = ? ORDER BY c.score DESC");
            ps.setString(1, userIdForAuth(authentication));
            ps.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
            List<PatInfoDTO> ret = new ArrayList<>();
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                PatInfoDTO next = new PatInfoDTO();
                next.setPat_id(rs.getString("pat_id"));
                next.setName(rs.getString("pat_name"));
                next.setDob(rs.getDate("pat_dob"));
                next.setBirth_gender_male_flag(rs.getBoolean("pat_birth_gender_male_flag"));
                String jgmt = rs.getString("judgement");
                if (jgmt != null) {
                    next.setInclusion(CohortInclusion.valueOf(jgmt.toUpperCase(Locale.ROOT)));
                } else {
                    next.setInclusion(CohortInclusion.UNJUDGED);
                }
                ret.add(next);
            }
            return ret;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort search result retrieval", e);
        }
    }

    public void writeCohortJudgement(Authentication authentication, UUID projectUID,
                                     String patId, CohortInclusion inclusion) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            // Manually check exists instead of using MERGE because not all SQL dialects support it
            PreparedStatement checkExists = conn.prepareStatement(
                    "SELECT pat_id FROM cat.judgements " +
                    "WHERE project_uid = ? AND pat_id = ? AND judger_id = ?");
            checkExists.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
            checkExists.setString(2, patId);
            checkExists.setString(3, userIdForAuth(authentication));
            if (checkExists.executeQuery().next()) {
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE cat.judgements " +
                                "SET judgement = ? " +
                                "WHERE project_uid = ? AND pat_id = ? AND judger_id = ?");
                ps.setString(1, inclusion.name());
                ps.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(3, patId);
                ps.setString(4, userIdForAuth(authentication));
                ps.execute();
            } else {
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO cat.judgements (project_uid, pat_id, judger_id, judgement) VALUES (?, ?, ?, ?)");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, patId);
                ps.setString(3, userIdForAuth(authentication));
                ps.setString(4, inclusion.name());
                ps.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort judgement write", e);
        }
    }

    public List<StructuredEvidenceDTO> getStructuredEvidenceForProject(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            // Manually check exists instead of using MERGE because not all SQL dialects support it
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT evidence_uid, evidence_item FROM cat.structured_evidence se JOIN ");
            return null; //TODO
        } catch (SQLException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on Structured evidence retrieval", e);
        }
    }

    public List<StructuredEvidenceDTO> getStructuredEvidenceForProjectCohortDefinitionNode(
            Authentication authentication, UUID projectUID, UUID nodeUID) {
        return null; // TODO
    }


    // ===== Utility/Service/Non-Returning Methods =====/

    // Instantiates the data source and tests for connection validity
    private void initDBConn(ApplicationConfiguration config) {
        try {
            this.datasource = new ComboPooledDataSource();
            this.datasource.setDriverClass(config.getPersistence().getDriverClass());
            this.datasource.setUser(config.getPersistence().getUser());
            this.datasource.setPassword(config.getPersistence().getPwd());
            this.datasource.setJdbcUrl(config.getPersistence().getUrl());
            this.datasource.setMaxStatements(180);
        } catch (PropertyVetoException e) {
            throw new IllegalArgumentException("Illegal persistence config", e);
        }
        try (Connection conn = this.datasource.getConnection()) {
            conn.prepareStatement("SELECT * FROM cat.projects").executeQuery();
        } catch (SQLException e) {
            throw new IllegalArgumentException("Could not instantiate connection to persistence database", e);
        }
    }

    // Gets user id from authentication
    private String userIdForAuth(Authentication auth) {
        return auth.getName().toUpperCase(Locale.ROOT); // TODO
    }
}
