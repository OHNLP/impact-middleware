package org.ohnlp.cat.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.dto.*;
import org.ohnlp.cat.dto.enums.CohortInclusion;
import org.ohnlp.cat.dto.enums.ProjectAuthorityGrant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;
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
    public List<ProjectDTO> getProjectList(Authentication authentication) throws IOException {
        List<ProjectDTO> ret = new ArrayList<>();
        try (Connection conn = this.datasource.getConnection()) {
            // If the user has any role grant, then user can at least view said project and it should be returned
            // If the project exists in project archive (pa.row_uid is not null) then do not return to user.
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT p.project_uid, p.project_name FROM cat.projects p " +
                            "JOIN cat.project_role_grants prg ON p.project_uid = prg.project_uid " +
                            "LEFT JOIN cat.project_archive pa ON p.project_uid = pa.project_uid " +
                            "WHERE prg.user_uid = ? AND pa.row_uid IS NULL"
            );
            ps.setString(1, userIdForAuth(authentication));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ProjectDTO project = new ProjectDTO();
                project.setName(rs.getString("project_name"));
                project.setUid(UUID.fromString(rs.getString("project_uid")));
                ret.add(project);
            }
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project List Retrieve", e);
        }
    }

    public ProjectDTO createProject(Authentication authentication, String projectName) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            ProjectDTO ret = new ProjectDTO();
            ret.setUid(UUID.randomUUID());
            ret.setName(projectName);
            // Begin atomic transaction block
            conn.setAutoCommit(false);
            // First, create the project itself
            PreparedStatement createProjectPS = conn.prepareStatement("INSERT INTO cat.projects (project_uid, project_name) VALUES (?, ?)");
            createProjectPS.setString(1, ret.getUid().toString().toUpperCase(Locale.ROOT));
            createProjectPS.setString(2, ret.getName());
            if (createProjectPS.executeUpdate() < 1) {
                throw new IllegalStateException("Failed to create project due to 0-change write to projects");
            }
            // Now create the authority grant
            PreparedStatement authorityGrantPS = conn.prepareStatement(
                    "INSERT INTO cat.project_role_grants (project_uid, user_uid, grant_type) VALUES (?, ?, ?)");
            authorityGrantPS.setString(1, ret.getUid().toString().toUpperCase(Locale.ROOT));
            authorityGrantPS.setString(2, userIdForAuth(authentication));
            authorityGrantPS.setString(3, ProjectAuthorityGrant.ADMIN.name());
            if (authorityGrantPS.executeUpdate() < 1) {
                throw new IllegalStateException("Failed to create project due to 0-change write to authority grants");
            }
            conn.commit();
            conn.setAutoCommit(true);
            return ret;
        } catch (Throwable e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project Creation", e);
        }
    }


    public ProjectDTO renameProject(Authentication authentication, UUID projectUID, String projectName) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.ADMIN)) {
                PreparedStatement updateProject = conn.prepareStatement("UPDATE cat.projects SET project_name = ? where project_uid = ?");
                updateProject.setString(1, projectName);
                updateProject.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
                if (updateProject.executeUpdate() < 1) {
                    throw new IllegalStateException("Failed to edit project due to 0-change write to projects");
                }
                ProjectDTO ret = new ProjectDTO();
                ret.setName(projectName);
                ret.setUid(projectUID);
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.ADMIN.name());
            }
        } catch (Throwable e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project List Retrieve", e);
        }
    }

    public Boolean updateRoleGrants(Authentication authentication, ProjectRoleDTO role) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, role.getProject_uid(), authentication, ProjectAuthorityGrant.WRITE)) {
                // Try update first
                PreparedStatement updateRoles = conn.prepareStatement("UPDATE cat.PROJECT_ROLE_GRANTS SET grant_type = ? WHERE project_uid = ? AND user_uid = ?");
                updateRoles.setString(1, role.getGrant().name());
                updateRoles.setString(2, role.getProject_uid().toString().toUpperCase(Locale.ROOT));
                updateRoles.setString(3, role.getUser_uid().toUpperCase(Locale.ROOT)); // TODO handle checking if user exists in system
                if (updateRoles.executeUpdate() < 1) { // No preexisting role for user on project
                    PreparedStatement insertRoles = conn.prepareStatement("INSERT INTO PROJECT_ROLE_GRANTS (project_uid, user_uid, grant_type) VALUES (?, ?, ?)");
                    insertRoles.setString(1, role.getProject_uid().toString().toUpperCase(Locale.ROOT));
                    insertRoles.setString(2, role.getUser_uid().toUpperCase(Locale.ROOT)); // TODO handle checking if user exists in system
                    insertRoles.setString(3, role.getGrant().name()); //
                    insertRoles.executeUpdate();
                }
                return true;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.WRITE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project Role Update", e);
        }
    }

    public Boolean archiveProject(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.ADMIN)) {
                PreparedStatement updateArchive = conn.prepareStatement("INSERT INTO cat.PROJECT_ARCHIVE (project_uid) VALUES (?)");
                updateArchive.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                if (updateArchive.executeUpdate() < 1) {
                    return false;
                }
                return true;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.ADMIN.name());
            }
        } catch (Throwable e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project Archive", e);
        }
    }

    public CriterionDefinitionDTO getProjectCriterion(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement("SELECT criterion, revision_date FROM cat.PROJECT_CRITERION p WHERE project_uid = ? ORDER BY revision_date DESC");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return om.get().readValue(rs.getString(1), CriterionDefinitionDTO.class);
                }
            }
            return null;
        } catch (SQLException | JsonProcessingException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project criteria retrieve", e);
        }
    }

    public Boolean writeProjectCriterion(Authentication authentication, UUID projectUID, CriterionDefinitionDTO def) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.WRITE)) {
                // Never try updating, instead always create new criterion definition with timestamp so that history is retained
                PreparedStatement ps = conn.prepareStatement("INSERT INTO cat.project_criterion (project_uid, criterion, revision_date) VALUES (?, ?, ?)"); // TODO
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, om.get().writeValueAsString(def));
                ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                ps.executeUpdate();
                return true;
            }
            return false;
        } catch (SQLException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project criteria write", e);
        }
    }

    // ===== Cohort Related Methods ===== //
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


    private boolean checkUserAuthority(Connection conn, UUID projectUID, Authentication authentication, ProjectAuthorityGrant minGrant) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("SELECT grant_type FROM cat.project_role_grants WHERE project_uid = ? AND user_uid = ?");
        ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
        ps.setString(2, userIdForAuth(authentication));
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            // pos/0 indicates match or greater (enum index comes at or before minGrant)
            if (minGrant.compareTo(ProjectAuthorityGrant.valueOf(rs.getString("grant_type"))) >= 0) {
                return true;
            }
        }
        return false;
    }


}
