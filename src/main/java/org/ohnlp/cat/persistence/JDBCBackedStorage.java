package org.ohnlp.cat.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.api.adjudication.CohortAdjudicationStatus;
import org.ohnlp.cat.api.adjudication.PatientAdjudicationStatus;
import org.ohnlp.cat.api.cohorts.CandidateInclusion;
import org.ohnlp.cat.api.cohorts.CohortCandidate;
import org.ohnlp.cat.api.criteria.*;
import org.ohnlp.cat.api.ehr.DataSourceInformation;
import org.ohnlp.cat.api.evidence.Evidence;
import org.ohnlp.cat.api.jobs.Job;
import org.ohnlp.cat.api.jobs.JobStatus;
import org.ohnlp.cat.api.projects.Project;
import org.ohnlp.cat.api.projects.ProjectAuthorityGrant;
import org.ohnlp.cat.api.projects.ProjectRole;
import org.ohnlp.cat.executors.JobExecutorManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

// TODO permissions checks for all functions?
@Component
public class JDBCBackedStorage {
    private final JobExecutorManager jobExecutor;
    private final ApplicationConfiguration config;
    private final String schema;
    private ComboPooledDataSource datasource;
    private ThreadLocal<ObjectMapper> om = ThreadLocal.withInitial(ObjectMapper::new);

    @Autowired
    public JDBCBackedStorage(ApplicationConfiguration config, JobExecutorManager jobExecutor) {
        this.schema = config.getPersistence().getSchema();
        initDBConn(config);
        this.jobExecutor = jobExecutor;
        this.config = config;
    }

    // ===== Project Management Methods ===== //
    public List<Project> getProjectList(Authentication authentication) throws IOException {
        List<Project> ret = new ArrayList<>();
        try (Connection conn = this.datasource.getConnection()) {
            // If the user has any role grant, then user can at least view said project and it should be returned
            // If the project exists in project archive (pa.row_uid is not null) then do not return to user.
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT p.project_uid, p.project_name, p.project_desc FROM " + schema + ".projects p " +
                            "JOIN " + schema + ".project_role_grants prg ON p.project_uid = prg.project_uid " +
                            "LEFT JOIN " + schema + ".project_archive pa ON p.project_uid = pa.project_uid " +
                            "WHERE prg.user_uid = ? AND pa.row_uid IS NULL"
            );
            ps.setString(1, userIdForAuth(authentication));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Project project = new Project();
                project.setName(rs.getString("project_name"));
                project.setUid(UUID.fromString(rs.getString("project_uid")));
                project.setDescription(rs.getString("project_desc"));
                ret.add(project);
            }
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();  // TODO log exceptions to db
            throw new IOException("Error on Project List Retrieve", e);
        }
    }

    public Project createProject(Authentication authentication, String projectName) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            Project ret = new Project();
            ret.setUid(UUID.randomUUID());
            ret.setName(projectName);
            // Begin atomic transaction block
            conn.setAutoCommit(false);
            // First, create the project itself
            PreparedStatement createProjectPS = conn.prepareStatement("INSERT INTO " + schema + ".projects (project_uid, project_name) VALUES (?, ?)");
            createProjectPS.setString(1, ret.getUid().toString().toUpperCase(Locale.ROOT));
            createProjectPS.setString(2, ret.getName());
            if (createProjectPS.executeUpdate() < 1) {
                throw new IllegalStateException("Failed to create project due to 0-change write to projects");
            }
            // Now create the authority grant
            PreparedStatement authorityGrantPS = conn.prepareStatement(
                    "INSERT INTO " + schema + ".project_role_grants (project_uid, user_uid, grant_type) VALUES (?, ?, ?)");
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


    public Project renameProject(Authentication authentication, UUID projectUID, String projectName) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.ADMIN)) {
                PreparedStatement updateProject = conn.prepareStatement("UPDATE " + schema + ".projects SET project_name = ? where project_uid = ?");
                updateProject.setString(1, projectName);
                updateProject.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
                if (updateProject.executeUpdate() < 1) {
                    throw new IllegalStateException("Failed to edit project due to 0-change write to projects");
                }
                Project ret = new Project();
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

    public Boolean updateRoleGrants(Authentication authentication, ProjectRole role) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, role.getProjectUID(), authentication, ProjectAuthorityGrant.WRITE)) {
                // Try update first
                PreparedStatement updateRoles = conn.prepareStatement("UPDATE " + schema + ".PROJECT_ROLE_GRANTS SET grant_type = ? WHERE project_uid = ? AND user_uid = ?");
                updateRoles.setString(1, role.getGrant().name());
                updateRoles.setString(2, role.getProjectUID().toString().toUpperCase(Locale.ROOT));
                updateRoles.setString(3, role.getUserUID().toUpperCase(Locale.ROOT)); // TODO handle checking if user exists in system
                if (updateRoles.executeUpdate() < 1) { // No preexisting role for user on project
                    PreparedStatement insertRoles = conn.prepareStatement("INSERT INTO " + schema + ".PROJECT_ROLE_GRANTS (project_uid, user_uid, grant_type) VALUES (?, ?, ?)");
                    insertRoles.setString(1, role.getProjectUID().toString().toUpperCase(Locale.ROOT));
                    insertRoles.setString(2, role.getUserUID().toUpperCase(Locale.ROOT)); // TODO handle checking if user exists in system
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
                PreparedStatement updateArchive = conn.prepareStatement("INSERT INTO " + schema + ".PROJECT_ARCHIVE (project_uid) VALUES (?)");
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

    public Criterion getProjectCriterion(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement("SELECT criterion, revision_date FROM " + schema + ".PROJECT_CRITERION p WHERE project_uid = ? ORDER BY revision_date DESC");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return om.get().readValue(rs.getString(1), Criterion.class);
                }
            }
            return null;
        } catch (SQLException | JsonProcessingException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project criteria retrieve", e);
        }
    }

    public Boolean writeProjectCriterion(Authentication authentication, UUID projectUID, Criterion def) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.WRITE)) {
                // Never try updating, instead always create new criterion definition with timestamp so that history is retained
                PreparedStatement ps = conn.prepareStatement("INSERT INTO " + schema + ".project_criterion (project_uid, criterion, revision_date) VALUES (?, ?, ?)"); // TODO
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

    public List<DataSourceInformation> getProjectDataSources(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement("SELECT data_sources FROM " + schema + ".project_data_sources WHERE project_uid = ?");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                rs.next();
                return om.get().readValue(rs.getString("data_source"), new TypeReference<>() {});
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project data sources read", e);
        }
    }

    public boolean writeProjectDataSources(Authentication authentication, UUID projectUID, List<DataSourceInformation> dataSources) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.WRITE)) {
                // Try update first
                PreparedStatement updateRoles = conn.prepareStatement("UPDATE " + schema + ".project_data_sources SET data_sources = ? WHERE project_uid = ?");
                updateRoles.setString(1, om.get().writeValueAsString(dataSources));
                updateRoles.setString(2, projectUID.toString().toUpperCase(Locale.ROOT));
                if (updateRoles.executeUpdate() < 1) { // No preexisting role for user on project
                    PreparedStatement insertRoles = conn.prepareStatement("INSERT INTO " + schema + ".project_data_sources (project_uid, data_sources) VALUES (?, ?)");
                    insertRoles.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                    insertRoles.setString(2,  om.get().writeValueAsString(dataSources));
                    insertRoles.executeUpdate();
                }
                return true;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.WRITE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project data sources write", e);
        }
    }

    // ===== Cohort Related Methods ===== //
    // Gets the current evaluated cohort for a given project/user. TODO consider adding pagination in returned results (sort by score)
    public List<CohortCandidate> getRetrievedCohort(Authentication authentication, UUID jobUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT c.person_uid, c.score, j.judgement " +
                                "FROM (SELECT * FROM " + schema + ".COHORT WHERE job_uid = ?) c LEFT JOIN (SELECT * FROM " + schema + ".COHORT_RELEVANCE WHERE judger_uid = ?) j" +
                                "     ON c.row_uid = j.cohort_row_uid ORDER BY c.score DESC");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, userIdForAuth(authentication));
                List<CohortCandidate> ret = new ArrayList<>();
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    CohortCandidate next = new CohortCandidate();
                    next.setPatUID(rs.getString("person_uid"));
                    String jgmt = rs.getString("judgement");
                    if (jgmt != null) {
                        next.setInclusion(CandidateInclusion.valueOf(jgmt.toUpperCase(Locale.ROOT)));
                    } else {
                        next.setInclusion(CandidateInclusion.UNJUDGED);
                    }
                    ret.add(next);
                }
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort search result retrieval", e);
        }
    }

    public Map<String, CandidateInclusion> getCohortRelevance(Authentication authentication, UUID jobUID, String... patientUIDs) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                Map<String, CandidateInclusion> ret = new HashMap<>();
                PreparedStatement ps = conn.prepareStatement("SELECT judgement " +
                        "FROM " + schema + ".COHORT c " +
                        "JOIN " + schema + ".COHORT_RELEVANCE cr ON c.row_uid = cr.cohort_row_uid " +
                        "WHERE c.job_uid = ? AND c.person_uid = ? AND cr.judger_uid = ?");
                for (String patientUID : patientUIDs) { // Do one-by-one search because not all JDBC drivers support IN clauses...
                    ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(2, patientUID);
                    ps.setString(3, userIdForAuth(authentication));
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        ret.put(patientUID, CandidateInclusion.valueOf(rs.getString("judgement")));
                    } else {
                        ret.put(patientUID, CandidateInclusion.UNJUDGED);
                    }
                }
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort judgement retrieval", e);
        }
    }

    public boolean writeCohortJudgement(Authentication authentication, UUID jobUID,
                                        String patId, CandidateInclusion inclusion) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.JUDGE)) {
                // Manually check exists instead of using MERGE because not all SQL dialects support it
                PreparedStatement checkExists = conn.prepareStatement(
                        "SELECT c.row_uid, cr.row_uid AS JUDGEMENT_ROW, cr.judgement FROM " + schema + ".COHORT c " +
                                "LEFT JOIN " + schema + ".COHORT_RELEVANCE cr ON c.row_uid = cr.cohort_row_uid AND cr.judger_uid = ?" +
                                "WHERE c.job_uid = ? AND c.person_uid = ?");
                checkExists.setString(1, userIdForAuth(authentication));
                checkExists.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                checkExists.setString(3, patId);
                ResultSet rs = checkExists.executeQuery();
                if (rs.next()) {
                    long rowUID = rs.getLong("row_uid");
                    if (rs.getString("judgement") != null) { // preexisting relevance
                        PreparedStatement ps = conn.prepareStatement(
                                "UPDATE " + schema + ".COHORT_RELEVANCE " +
                                        "SET judgement = ? " +
                                        "WHERE row_uid = ?");
                        ps.setString(1, inclusion.name());
                        ps.setLong(2, rs.getLong("JUDGEMENT_ROW"));
                        return ps.executeUpdate() > 0;
                    } else {
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO " + schema + ".COHORT_RELEVANCE (cohort_row_uid, judger_uid, judgement) VALUES (?, ?, ?)");
                        ps.setLong(1, rowUID);
                        ps.setString(2, userIdForAuth(authentication));
                        ps.setString(3, inclusion.name());
                        return ps.executeUpdate() > 0;
                    }
                } else {
                    throw new IllegalStateException("Attempted to create judgement on patient " + patId + " that does not exist in cohort for job " + jobUID);
                }
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.JUDGE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on cohort judgement write", e);
        }
    }

    public Map<String, CriterionInfo> getCriterionMatchStatus(Authentication authentication, UUID jobUID, String personUID) throws IOException {
        Set<String> nodeUIDs = new HashSet<>();
        Map<String, CriterionInfo> judgements = new ConcurrentHashMap<>();
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT pc.criterion " +
                                "FROM " + schema + ".AUDIT_LOG al " +
                                "JOIN " + schema + ".PROJECT_CRITERION pc ON al.criterion_uid = pc.row_uid " +
                                "WHERE al.job_uid = ?"
                );
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                Criterion def;
                if (rs.next()) {
                    def = om.get().readValue(rs.getString("criterion"), Criterion.class);
                } else {
                    throw new IllegalStateException("No definition stored for job");
                }

                recursSearchNodeUIDsFromDef(def, nodeUIDs);
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on current criterion status retrieval", e);
        }
        nodeUIDs.parallelStream().forEach(node_uid -> {
            try (Connection conn = this.datasource.getConnection()){
                PreparedStatement nodeRetrieval = conn.prepareStatement(
                        "SELECT nr.judgement, nr.user_comment FROM " + schema + ".NODE_RELEVANCE nr WHERE nr.job_uid = ? AND nr.node_uid = ? AND person_uid = ? AND nr.judger_uid = ? ");
                PreparedStatement evidenceRetrieval = conn.prepareStatement(
                        "SELECT DISTINCT e.evidence_uid, er.judgement FROM " + schema + ".EVIDENCE e " +
                                "LEFT JOIN " + schema + ".EVIDENCE_RELEVANCE er ON e.row_uid = er.evidence_row_uid AND er.judger_uid = ? " +
                                "WHERE e.job_uid = ? AND e.node_uid = ? AND e.person_uid = ?");
                CriterionInfo nodeJudgement = new CriterionInfo();
                // Check for a node-level judgement first. If there is one, override everything
                nodeRetrieval.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                nodeRetrieval.setString(2, node_uid.toUpperCase(Locale.ROOT));
                nodeRetrieval.setString(3, personUID);
                nodeRetrieval.setString(4, userIdForAuth(authentication));
                ResultSet rs2 = nodeRetrieval.executeQuery();
                if (rs2.next()) {
                    String judgement = rs2.getString("judgement");
                    if (judgement != null) {
                        nodeJudgement.setJudgement(CriterionJudgement.valueOf(judgement));
                    }
                    nodeJudgement.setComment(rs2.getString("user_comment"));
                }
                rs2.close();
                if (nodeJudgement.getJudgement() != null) {
                    judgements.put(node_uid, nodeJudgement);
                    return;
                }
                evidenceRetrieval.setString(1, userIdForAuth(authentication));
                evidenceRetrieval.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                evidenceRetrieval.setString(3, node_uid.toUpperCase(Locale.ROOT));
                evidenceRetrieval.setString(4, personUID);

                rs2 = evidenceRetrieval.executeQuery();
                boolean found = false;
                CriterionJudgement evidenceJudgementState = CriterionJudgement.NO_EVIDENCE_FOUND;
                // First, determine if there is a user judgement overriding default algorithmic matches
                while (rs2.next()) {
                    found = true;
                    String judgement = rs2.getString("judgement");
                    if (judgement != null) {
                        CriterionJudgement parsedJudgement = CriterionJudgement.valueOf(judgement);
                        if (evidenceJudgementState.compareTo(parsedJudgement) > 0) { // Lower priority than the parsed judgement
                            evidenceJudgementState = parsedJudgement;
                        }
                    }
                }
                // Now adjust if there are no user judgements but there is evidence
                if (found && (evidenceJudgementState.equals(CriterionJudgement.NO_EVIDENCE_FOUND))) {
                    evidenceJudgementState = CriterionJudgement.EVIDENCE_FOUND;
                }
                rs2.close();
                nodeJudgement.setJudgement(evidenceJudgementState);
                judgements.put(node_uid, nodeJudgement);
            } catch (SQLException e) {
                throw new RuntimeException("Error retrieving evidence judgement states", e);
            }
        });
        return judgements;
    }

    private void recursSearchNodeUIDsFromDef(Criterion def, Set<String> nodeUIDs) {
        if (def instanceof EntityCriterion) {
            nodeUIDs.add(def.getNodeUID().toString().toUpperCase(Locale.ROOT));
        } else if (def instanceof LogicalCriterion) {
            for (Criterion child : ((LogicalCriterion) def).getChildren()) {
                recursSearchNodeUIDsFromDef(child, nodeUIDs);
            }
        } else {
            throw new UnsupportedOperationException("Unknown criterion object type " + def.getClass().getName());
        }
    }


    public Map<String, CriterionInfo> setCriterionMatchStatus(Authentication authentication, UUID jobUID, UUID nodeUID, String personUID, CriterionInfo judgement) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.JUDGE)) {
                PreparedStatement ps = conn.prepareStatement("UPDATE " + schema + ".NODE_RELEVANCE SET judgement = ?, user_comment = ? WHERE job_uid = ? AND node_uid = ? AND person_uid = ? AND judger_uid = ? ");
                ps.setString(1, judgement.getJudgement() == null ? null : judgement.getJudgement().name());
                ps.setString(2, judgement.getComment());
                ps.setString(3, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(4, nodeUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(5, personUID);
                ps.setString(6, userIdForAuth(authentication));
                if (ps.executeUpdate() == 0) { // No preexisting row
                    ps = conn.prepareStatement("INSERT INTO " + schema + ".NODE_RELEVANCE (job_uid, node_uid, person_uid, judger_uid, judgement, user_comment) VALUES (?, ?, ?, ?, ?, ?)");
                    ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(2, nodeUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(3, personUID);
                    ps.setString(4, userIdForAuth(authentication));
                    ps.setString(5, judgement.getJudgement() == null ? null : judgement.getJudgement().name());
                    ps.setString(6, judgement.getComment());
                    ps.executeUpdate();
                }
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.JUDGE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on current criterion status write", e);
        }
        return getCriterionMatchStatus(authentication, jobUID, personUID);
    }

    public Criterion getJobCriterion(Authentication authentication, UUID jobUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement("SELECT c.criterion FROM " + schema + ".AUDIT_LOG a JOIN " + schema + ".PROJECT_CRITERION c ON a.criterion_uid = c.row_uid WHERE job_uid = ?");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return om.get().readValue(rs.getString(1), Criterion.class);
                }
            }
            return null;
        } catch (SQLException | JsonProcessingException e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project criteria retrieve", e);
        }
    }
    public List<DataSourceInformation> getProjectDataSourcesByJobUID(Authentication authentication, UUID jobUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            UUID projectUID = getProjectUIDForJob(conn, jobUID);
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement("SELECT data_sources FROM " + schema + ".project_data_sources WHERE project_uid = ?");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                rs.next();
                return om.get().readValue(rs.getString("data_source"), new TypeReference<>() {});
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on project data sources read", e);
        }
    }

    // ===== Evidence Related Methods ===== //

    public Map<String, CriterionJudgement> getEvidenceRelevance(Authentication authentication, UUID jobUID, UUID nodeUID, String... evidenceUIDs) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                Map<String, CriterionJudgement> ret = new HashMap<>();
                PreparedStatement ps = conn.prepareStatement("SELECT er.judgement " +
                        "FROM " + schema + ".EVIDENCE e " +
                        "JOIN " + schema + ".EVIDENCE_RELEVANCE er ON e.row_uid = er.evidence_row_uid " +
                        "WHERE e.job_uid = ? AND e.evidence_uid = ? AND e.node_uid = ? AND er.judger_uid = ?");
                for (String evidenceUID : evidenceUIDs) { // Do one-by-one search because not all JDBC drivers support IN clauses...
                    ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(2, evidenceUID);
                    ps.setString(3, nodeUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(4, userIdForAuth(authentication));
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        ret.put(evidenceUID, CriterionJudgement.valueOf(rs.getString("judgement")));
                    } else {
                        ret.put(evidenceUID, CriterionJudgement.UNJUDGED);
                    }
                }
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on evidence judgement retrieval", e);
        }
    }

    public Boolean writeEvidenceJudgement(Authentication authentication, UUID jobUID, UUID nodeUID, String evidenceUID, CriterionJudgement judgement) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.JUDGE)) {
                // Manually check exists instead of using MERGE because not all SQL dialects support it
                PreparedStatement checkExists = conn.prepareStatement(
                        "SELECT e.row_uid, er.row_uid AS JUDGEMENT_ROW, er.judgement FROM " + schema + ".EVIDENCE e " +
                                "LEFT JOIN " + schema + ".EVIDENCE_RELEVANCE er ON e.row_uid = er.evidence_row_uid AND er.judger_uid = ? " +
                                "WHERE e.job_uid = ? AND e.node_uid = ? AND e.evidence_uid = ? ");
                checkExists.setString(1, userIdForAuth(authentication));
                checkExists.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                checkExists.setString(3, nodeUID.toString().toUpperCase(Locale.ROOT));
                checkExists.setString(4, evidenceUID);
                ResultSet rs = checkExists.executeQuery();
                if (rs.next()) {
                    long rowUID = rs.getLong("row_uid");
                    if (rs.getString("judgement") != null) { // preexisting relevance
                        PreparedStatement ps = conn.prepareStatement(
                                "UPDATE " + schema + ".EVIDENCE_RELEVANCE " +
                                        "SET judgement = ? " +
                                        "WHERE row_uid = ?");
                        ps.setString(1, judgement.name());
                        ps.setLong(2, rs.getLong("JUDGEMENT_ROW"));
                        return ps.executeUpdate() > 0;
                    } else {
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO " + schema + ".EVIDENCE_RELEVANCE (evidence_row_uid, judger_uid, judgement) VALUES (?, ?, ?)");
                        ps.setLong(1, rowUID);
                        ps.setString(2, userIdForAuth(authentication));
                        ps.setString(3, judgement.name());
                        return ps.executeUpdate() > 0;
                    }
                } else {
                    throw new IllegalStateException("Attempted to create judgement on evidence " + evidenceUID + " that does not exist in job " + jobUID + " and node " + nodeUID);
                }
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.JUDGE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on evidence judgement write", e);
        }
    }

    public List<Evidence> getEvidenceForNode(
            Authentication authentication, UUID jobUID, UUID nodeUID, String personUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT evidence_uid, score FROM " + schema + ".EVIDENCE WHERE job_uid = ? AND node_uid = ? AND person_uid = ? ORDER BY score DESC");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, nodeUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(3, personUID);
                ResultSet rs = ps.executeQuery();
                List<Evidence> ret = new ArrayList<>();
                while (rs.next()) {
                    Evidence evidence = new Evidence();
                    evidence.setEvidenceUID(rs.getString("evidence_uid"));
                    evidence.setScore(rs.getDouble("score"));
                    ret.add(evidence);
                }
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on Structured evidence retrieval", e);
        }
    }

    // ===== Job Related Methods =====/

    public List<Job> getJobsForUser(Authentication authentication) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            // No need to check user authority to read own jobs
            List<Job> ret = new ArrayList<>();
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT job_uid, project_uid, start_dtm, user_uid, job_status " +
                            "FROM " + schema + ".AUDIT_LOG WHERE user_uid = ? AND archived != 1 ORDER BY start_dtm DESC");
            ps.setString(1, userIdForAuth(authentication));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Job info = new Job();
                info.setProjectUID(UUID.fromString(rs.getString("project_uid")));
                info.setStartDate(rs.getTimestamp("start_dtm"));
                info.setStatus(JobStatus.forCode(rs.getInt("job_status")));
                info.setJobUID(UUID.fromString(rs.getString("job_uid")));
                ret.add(info);
            }
            return ret;

        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job retrieval", e);
        }
    }

    public List<Job> getJobsForProject(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            List<Job> ret = new ArrayList<>();
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.READ)) {
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT job_uid, start_dtm, user_uid, job_status FROM " + schema + ".AUDIT_LOG " +
                                "WHERE project_uid = ? AND archived != 1 ORDER BY start_dtm DESC");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Job info = new Job();
                    info.setProjectUID(projectUID);
                    info.setStartDate(rs.getTimestamp("start_dtm"));
                    info.setStatus(JobStatus.forCode(rs.getInt("job_status")));
                    info.setJobUID(UUID.fromString(rs.getString("job_uid")));
                    ret.add(info);
                }
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }

        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job retrieval", e);
        }
    }

    public Job runJob(Authentication authentication, UUID projectUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, projectUID, authentication, ProjectAuthorityGrant.EXECUTE)) {
                // Use Current/Latest criterion by date
                PreparedStatement ps = conn.prepareStatement("SELECT row_uid, criterion FROM " + schema + ".PROJECT_CRITERION p WHERE project_uid = ? ORDER BY revision_date DESC");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                long criterionRowID;
                Criterion criterion;
                if (rs.next()) {
                    criterionRowID = rs.getLong("row_uid");
                    criterion = om.get().readValue(rs.getString("criterion"), Criterion.class);
                } else {
                    throw new IllegalArgumentException("Project " + projectUID + " has no active criterion stored");
                }
                rs.close();
                // Create the audit record first so that if the executor starts right away it has information to retrieve
                UUID jobUID = UUID.randomUUID();
                long jobTimestamp = System.currentTimeMillis();
                ps = conn.prepareStatement("INSERT INTO " + schema + ".AUDIT_LOG (project_uid, job_uid, criterion_uid, user_uid, start_dtm, job_status) VALUES (?, ?, ?, ?, ?, ?)");
                ps.setString(1, projectUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.setLong(3, criterionRowID);
                ps.setString(4, userIdForAuth(authentication));
                ps.setTimestamp(5, new Timestamp(jobTimestamp));
                ps.setInt(6, JobStatus.QUEUED.getCode());
                if (ps.executeUpdate() < 1) {
                    throw new IOException("No rows updated in audit log for job creation");
                }
                // Now actually attempt to start the job
                String executorJobUID;
                try {
                    executorJobUID = jobExecutor.getExecutor().startJob(jobUID, criterion, config.getApplicationURL());
                } catch (Throwable t) {
                    // Delete the audit record as the job never actually started
                    ps = conn.prepareStatement("DELETE FROM " + schema + ".AUDIT_LOG WHERE job_uid = ?");
                    ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                    ps.executeUpdate();
                    throw new IOException("Failed to start job execution", t);
                }
                ps = conn.prepareStatement("UPDATE " + schema + ".AUDIT_LOG SET executor_job_uid = ? where job_uid = ?");
                ps.setString(1, executorJobUID);
                ps.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.executeUpdate(); // TODO handle update fails(?) Problem is job is eventually still queued anyways at this point
                Job ret = new Job();
                ret.setJobUID(jobUID);
                ret.setProjectUID(projectUID);
                ret.setStartDate(new Date(jobTimestamp));
                ret.setStatus(JobStatus.QUEUED);
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.EXECUTE.name());
            }

        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job creation", e);
        }
    }

    public boolean setJobStatus(Authentication authentication, UUID jobUID, JobStatus status) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.EXECUTE)) {
                PreparedStatement ps = conn.prepareStatement("UPDATE " + schema + ".AUDIT_LOG SET job_status = ? WHERE job_uid = ?");
                ps.setInt(1, status.getCode());
                ps.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                return ps.executeUpdate() > 0;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.EXECUTE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job cancel", e);
        }
    }

    public boolean cancelJobRecord(Authentication authentication, UUID jobUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.EXECUTE)) {
                PreparedStatement ps = conn.prepareStatement("UPDATE " + schema + ".AUDIT_LOG SET job_status = -2 WHERE job_uid = ? AND job_status NOT IN (3, -1)");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                return ps.executeUpdate() > 0;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.EXECUTE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job cancel", e);
        }
    }

    public boolean archiveJobRecord(Authentication authentication, UUID jobUID) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.WRITE)) {
                PreparedStatement ps = conn.prepareStatement("UPDATE " + schema + ".AUDIT_LOG SET archived = 1 WHERE job_uid = ? AND job_status IN (3, -1)");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                return ps.executeUpdate() > 0;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.WRITE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job archive", e);
        }
    }

    // ===== Adjudication Related Methods =====/
    public Map<String, CohortAdjudicationStatus> getCohortAdjudicationState(Authentication auth, UUID jobUID) throws IOException {
        Map<String, CohortAdjudicationStatus> ret = new LinkedHashMap<>();
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), auth, ProjectAuthorityGrant.WRITE)) {
                PreparedStatement ps = conn.prepareStatement(
                        "WITH PATS AS (SELECT row_uid, person_uid, score FROM " + schema + ".COHORT WHERE job_uid = ?), " +
                                "     COHORT_REL_GROUPED AS (SELECT cohort_row_uid, judgement, count(*) AS cnt FROM " + schema + ".COHORT_RELEVANCE GROUP BY cohort_row_uid, judgement), " +
                                "     OVERRIDES AS (SELECT cohort_row_uid, judgment FROM " + schema + ".COHORT_RELEVANCE_ADJUDICATIONS) " +
                                "SELECT p.person_uid, c.judgement, c.cnt, o.judgment AS override FROM PATS p " +
                                "LEFT JOIN COHORT_REL_GROUPED c ON p.row_uid = c.cohort_row_uid " +
                                "LEFT JOIN OVERRIDES o ON o.cohort_row_uid = p.row_uid " +
                                "ORDER BY p.score DESC");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String personID = rs.getString("person_uid");
                    ret.computeIfAbsent(personID, k -> {
                        CohortAdjudicationStatus status = new CohortAdjudicationStatus();
                        status.setStatus(new HashMap<>());
                        return status;
                    });
                    String jdgmt = rs.getString("judgement");
                    // If no judgements, we set adjudication to 0 after
                    if (jdgmt != null) {
                        ret.get(personID).getStatus().put(CandidateInclusion.valueOf(jdgmt), rs.getInt("cnt"));
                    }
                    String override = rs.getString("override");
                    if (override != null) {
                        ret.get(personID).setTiebreakerOverride(CandidateInclusion.valueOf(override));
                    }
                }
                ret.forEach((pat_uid, status) -> {
                    status.setNumAdjudicators(status.getStatus().values().stream().reduce(Integer::sum).orElse(0));
                });
                return ret;
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on job cohort adjudication", e);
        }
    }

    public Map<String, CohortAdjudicationStatus> setAdjudicationOverrideCohort(Authentication authentication, UUID jobUID, String personUID, CandidateInclusion status) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.JUDGE)) {
                // Get the cohort row id
                PreparedStatement ps = conn.prepareStatement("SELECT row_uid FROM " + schema + ".COHORT WHERE job_uid = ?");
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                int row_uid = -1;
                if (rs.next()) {
                    row_uid = rs.getInt(1);
                } else {
                    throw new RuntimeException("Person UID " + personUID + " not found in cohort for job " + jobUID);
                }
                ps = conn.prepareStatement("UPDATE " + schema + ".COHORT_RELEVANCE_ADJUDICATIONS SET judgement = ? WHERE cohort_row_uid = ?");
                ps.setString(1, status.name());
                ps.setInt(2, row_uid);
                if (ps.executeUpdate() == 0) { // No preexisting row
                    ps = conn.prepareStatement("INSERT INTO " + schema + ".COHORT_RELEVANCE_ADJUDICATIONS (cohort_row_uid, judgement) VALUES (?, ?)");
                    ps.setInt(1, row_uid);
                    ps.setString(2, status.name());
                    ps.executeUpdate();
                }
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.JUDGE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on setting cohort adjudication state", e);
        }
        return getCohortAdjudicationState(authentication, jobUID);
    }

    public Map<String, PatientAdjudicationStatus> getCriteriaAdjudicationState(Authentication auth, UUID jobUID, String personUID) throws IOException {
        Set<String> nodeUIDs = new HashSet<>();
        Map<String, PatientAdjudicationStatus> judgements = new ConcurrentHashMap<>();
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), auth, ProjectAuthorityGrant.JUDGE)) {
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT pc.criterion " +
                                "FROM " + schema + ".AUDIT_LOG al " +
                                "JOIN " + schema + ".PROJECT_CRITERION pc ON al.criterion_uid = pc.row_uid " +
                                "WHERE al.job_uid = ?"
                );
                ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                ResultSet rs = ps.executeQuery();
                Criterion def;
                if (rs.next()) {
                    def = om.get().readValue(rs.getString("criterion"), Criterion.class);
                } else {
                    throw new IllegalStateException("No definition stored for job");
                }

                recursSearchNodeUIDsFromDef(def, nodeUIDs);
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.READ.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on current criterion status retrieval", e);
        }
        nodeUIDs.parallelStream().forEach(node_uid -> {
            try (Connection conn = this.datasource.getConnection()){
                PreparedStatement nodeRetrieval = conn.prepareStatement(
                        "SELECT nr.judgement, nr.user_comment FROM " + schema + ".NODE_RELEVANCE nr WHERE nr.job_uid = ? AND nr.node_uid = ? AND person_uid = ?");
                // Check for a node-level judgement first. If there is one, override everything
                nodeRetrieval.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                nodeRetrieval.setString(2, node_uid.toUpperCase(Locale.ROOT));
                nodeRetrieval.setString(3, personUID);
                ResultSet rs2 = nodeRetrieval.executeQuery();
                Map<CriterionJudgement, Integer> counts = new HashMap<>();
                while (rs2.next()) {
                    String judgement = rs2.getString("judgement");
                    counts.merge(CriterionJudgement.valueOf(judgement), 1, Integer::sum);
                }
                rs2.close();
                PreparedStatement overrideRetrieval = conn.prepareStatement("SELECT judgement FROM " + schema + ".NODE_RELEVANCE_OVERRIDE WHERE job_uid = ? AND node_uid = ? AND person_uid = ?");
                overrideRetrieval.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                overrideRetrieval.setString(2, node_uid.toUpperCase(Locale.ROOT));
                overrideRetrieval.setString(3, personUID);
                rs2 = overrideRetrieval.executeQuery();
                CriterionJudgement override = rs2.next() ? CriterionJudgement.valueOf(rs2.getString("judgement")) : null;
                rs2.close();
                judgements.computeIfAbsent(node_uid, k -> {
                    PatientAdjudicationStatus status = new PatientAdjudicationStatus();
                    status.setStatus(counts);
                    status.setNumAdjudicators(counts.values().stream().reduce(Integer::sum).orElse(0));
                    if (override != null) {
                        status.setTiebreakerOverride(override);
                    }
                    return status;
                });
            } catch (SQLException e) {
                throw new RuntimeException("Error retrieving evidence judgement states", e);
            }
        });
        return judgements;
    }


    public Map<String, PatientAdjudicationStatus> setAdjudicationOverrideCriterion(Authentication authentication, UUID jobUID, UUID nodeUID, String personUID, CriterionJudgement status) throws IOException {
        try (Connection conn = this.datasource.getConnection()) {
            if (checkUserAuthority(conn, getProjectUIDForJob(conn, jobUID), authentication, ProjectAuthorityGrant.JUDGE)) {
                // Get the cohort row id
                PreparedStatement ps = conn.prepareStatement("UPDATE " + schema + ".NODE_RELEVANCE_ADJUDICATIONS SET judgement = ? WHERE job_uid = ? AND node_uid = ? AND person_uid = ?");
                ps.setString(1, status.name());
                ps.setString(2, jobUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(3, nodeUID.toString().toUpperCase(Locale.ROOT));
                ps.setString(4, personUID);
                if (ps.executeUpdate() == 0) { // No preexisting row
                    ps = conn.prepareStatement("INSERT INTO " + schema + ".NODE_RELEVANCE_ADJUDICATIONS (job_uid, node_uid, person_uid, judgement) VALUES (?, ?, ?, ?)");
                    ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(2, nodeUID.toString().toUpperCase(Locale.ROOT));
                    ps.setString(3, personUID);
                    ps.setString(4, status.name());
                    ps.executeUpdate();
                }
            } else {
                throw new IllegalAccessException("User does not have the required role " + ProjectAuthorityGrant.JUDGE.name());
            }
        } catch (Throwable e) {
            e.printStackTrace(); // TODO log exceptions to DB
            throw new IOException("Error on setting cohort adjudication state", e);
        }
        return getCriteriaAdjudicationState(authentication, jobUID, personUID);
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
            conn.prepareStatement("SELECT * FROM " + schema + ".projects").executeQuery();
        } catch (SQLException e) {
            throw new IllegalArgumentException("Could not instantiate connection to persistence database", e);
        }
    }

    // Gets user id from authentication
    private String userIdForAuth(Authentication auth) {
        return auth.getName().toUpperCase(Locale.ROOT); // TODO
    }


    private boolean checkUserAuthority(Connection conn, UUID projectUID, Authentication authentication, ProjectAuthorityGrant minGrant) throws SQLException {
        if (userIdForAuth(authentication).equals(config.getBackendCallbackUsername().toUpperCase(Locale.ROOT))) {
            return true;
        }
        PreparedStatement ps = conn.prepareStatement("SELECT grant_type FROM " + schema + ".project_role_grants WHERE project_uid = ? AND user_uid = ?");
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

    private UUID getProjectUIDForJob(Connection conn, UUID jobUID) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
                "SELECT al.project_uid " +
                        "FROM " + schema + ".AUDIT_LOG al " +
                        "WHERE al.job_uid = ?"
        );
        ps.setString(1, jobUID.toString().toUpperCase(Locale.ROOT));
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return UUID.fromString(rs.getString(1));
        } else {
            throw new IllegalArgumentException("JOB ID " + jobUID + " caused error on project UID resolution");
        }
    }
}
