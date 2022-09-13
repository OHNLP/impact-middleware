package org.ohnlp.cat.controllers;

import org.ohnlp.cat.dto.EvidenceDTO;
import org.ohnlp.cat.dto.PatInfoDTO;
import org.ohnlp.cat.dto.enums.PatientJudgementState;
import org.ohnlp.cat.dto.enums.NodeMatchState;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Controller
@RequestMapping("/_cohorts")
public class CohortController {

    private final JDBCBackedStorage storage;

    @Autowired
    public CohortController(JDBCBackedStorage storage) {
        this.storage = storage;
    }


    @GetMapping("/")
    public @ResponseBody
    List<PatInfoDTO> getRetrievedCohort(Authentication authentication,
                                        @RequestParam(name = "job_uid") String jobUID) {
        try {
            return storage.getRetrievedCohort(authentication, UUID.fromString(jobUID));
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/evidence")
    public @ResponseBody
    List<EvidenceDTO> getEvidenceForNode(Authentication authentication,
                                         @RequestParam(name = "job_uid") String jobUID,
                                         @RequestParam(name = "node_uid") String nodeUID,
                                         @RequestParam(name = "person_uid") String personUID) {
        try {
            return storage.getEvidenceForNode(
                    authentication,
                    UUID.fromString(jobUID),
                    UUID.fromString(nodeUID),
                    personUID
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/relevance")
    public @ResponseBody
    Map<String, PatientJudgementState> getCohortRelevance(Authentication authentication,
                                                          @RequestParam(name = "job_uid") String jobUID,
                                                          @RequestParam(name = "patient_uid") String... patientUIDs) {
        try {
            return storage.getCohortRelevance(
                    authentication,
                    UUID.fromString(jobUID),
                    patientUIDs
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @PostMapping("/relevance")
    public @ResponseBody
    Boolean writeCohortRelevance(Authentication authentication,
                                 @RequestParam(name = "job_uid") String jobUID,
                                 @RequestParam(name = "patient_uid") String patientUID,
                                 @RequestParam(name = "judgement") PatientJudgementState judgement) {
        try {
            return storage.writeCohortJudgement(
                    authentication,
                    UUID.fromString(jobUID),
                    patientUID,
                    judgement
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/evidence")
    public @ResponseBody
    Map<String, NodeMatchState> getEvidenceRelevance(Authentication authentication,
                                                     @RequestParam(name = "job_uid") String jobUID,
                                                     @RequestParam(name = "node_uid") String nodeUID,
                                                     @RequestParam(name = "evidence_uid") String... evidenceUIDs) {
        try {
            return storage.getEvidenceRelevance(
                    authentication,
                    UUID.fromString(jobUID),
                    UUID.fromString(nodeUID),
                    evidenceUIDs
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @PostMapping("/evidence")
    public @ResponseBody
    Boolean writeEvidenceRelevance(Authentication authentication,
                                   @RequestParam(name = "job_uid") String jobUID,
                                   @RequestParam(name = "node_uid") String nodeUID,
                                   @RequestParam(name = "evidence_uid") String evidenceUID,
                                   @RequestParam(name = "judgement") NodeMatchState judgement) {
        try {
            return storage.writeEvidenceJudgement(
                    authentication,
                    UUID.fromString(jobUID),
                    UUID.fromString(nodeUID),
                    evidenceUID,
                    judgement
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/criterion_match_status")
    public @ResponseBody
    Map<String, NodeMatchState> getCriterionMatchStatus(Authentication authentication,
                                                        @RequestParam(name = "job_uid") String jobUID) {
        try {
            return storage.getCriterionMatchStatus(authentication, UUID.fromString(jobUID));
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }
}
