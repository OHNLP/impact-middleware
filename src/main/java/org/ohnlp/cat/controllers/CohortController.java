package org.ohnlp.cat.controllers;

import org.ohnlp.cat.dto.EvidenceDTO;
import org.ohnlp.cat.dto.JudgementDTO;
import org.ohnlp.cat.dto.PatInfoDTO;
import org.ohnlp.cat.dto.enums.PatientJudgementState;
import org.ohnlp.cat.dto.enums.JudgementState;
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
                                        @RequestParam(name = "job_uid") UUID jobUID) {
        try {
            return storage.getRetrievedCohort(authentication, jobUID);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/node_evidence")
    public @ResponseBody
    List<EvidenceDTO> getEvidenceForNode(Authentication authentication,
                                         @RequestParam(name = "job_uid") UUID jobUID,
                                         @RequestParam(name = "node_uid") UUID nodeUID,
                                         @RequestParam(name = "person_uid") String personUID) {
        try {
            return storage.getEvidenceForNode(
                    authentication,
                    jobUID,
                    nodeUID,
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
                                                          @RequestParam(name = "job_uid") UUID jobUID,
                                                          @RequestParam(name = "patient_uid") String... patientUIDs) {
        try {
            return storage.getCohortRelevance(
                    authentication,
                    jobUID,
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
                                 @RequestParam(name = "job_uid") UUID jobUID,
                                 @RequestParam(name = "patient_uid") String patientUID,
                                 @RequestParam(name = "judgement") PatientJudgementState judgement) {
        try {
            return storage.writeCohortJudgement(
                    authentication,
                    jobUID,
                    patientUID,
                    judgement
            );
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @GetMapping("/node_matchstates")
    public @ResponseBody
    Map<String, JudgementState> getEvidenceRelevance(Authentication authentication,
                                                     @RequestParam(name = "job_uid") UUID jobUID,
                                                     @RequestParam(name = "node_uid") UUID nodeUID,
                                                     @RequestParam(name = "evidence_uid") String... evidenceUIDs) {
        try {
            return storage.getEvidenceRelevance(
                    authentication,
                    jobUID,
                    nodeUID,
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
                                   @RequestParam(name = "job_uid") UUID jobUID,
                                   @RequestParam(name = "node_uid") UUID nodeUID,
                                   @RequestParam(name = "evidence_uid") String evidenceUID,
                                   @RequestParam(name = "judgement") JudgementState judgement) {
        try {
            return storage.writeEvidenceJudgement(
                    authentication,
                    jobUID,
                    nodeUID,
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
    Map<String, JudgementDTO> getCriterionMatchStatus(Authentication authentication,
                                                        @RequestParam(name = "job_uid") UUID jobUID) {
        try {
            return storage.getCriterionMatchStatus(authentication, jobUID);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @PostMapping("/criterion_match_status")
    public @ResponseBody
    Map<String, JudgementDTO> setCriterionMatchStatus(Authentication authentication,
                                                        @RequestParam(name = "job_uid") UUID jobUID,
                                                        @RequestParam(name = "node_uid") UUID nodeUID,
                                                        @RequestBody JudgementDTO judgement) {
        try {
            return storage.setCriterionMatchStatus(authentication, jobUID, nodeUID, judgement);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }
}
