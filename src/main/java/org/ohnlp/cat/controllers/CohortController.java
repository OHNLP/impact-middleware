package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.cohorts.CandidateInclusion;
import org.ohnlp.cat.api.cohorts.CohortCandidate;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.Criterion;
import org.ohnlp.cat.api.criteria.CriterionInfo;
import org.ohnlp.cat.api.criteria.CriterionJudgement;
import org.ohnlp.cat.api.evidence.Evidence;
import org.ohnlp.cat.evidence.EvidenceProvider;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Tag(name = "Cohort Controller", description = "Cohort and Evidence Related Methods, Including Results and Evidence Judgements")
@Controller
@RequestMapping("/_cohorts")
public class CohortController {

    private final JDBCBackedStorage storage;
    private EvidenceProvider evidenceProvider = new EvidenceProvider(); // TODO autowire

    @Autowired
    public CohortController(JDBCBackedStorage storage) {
        this.storage = storage;
    }


    @Operation(summary = "Get a Listing of Cohort Candidate by Job UID")
    @GetMapping("/")
    public @ResponseBody
    List<CohortCandidate> getRetrievedCohort(Authentication authentication,
                                             @RequestParam(name = "job_uid") UUID jobUID) {
        try {
            return storage.getRetrievedCohort(authentication, jobUID);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @Operation(summary = "Get a Listing of Evidence for a given Job/Patient on a Specific Criterion (node) UID")
    @GetMapping("/node_evidence")
    public @ResponseBody
    List<Evidence> getEvidenceForNode(Authentication authentication,
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

    @Operation(summary = "Gets current inclusion status for the given job and list of candidate patient UIDs")
    @GetMapping("/relevance")
    public @ResponseBody
    Map<String, CandidateInclusion> getCohortRelevance(Authentication authentication,
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

    @Operation(summary = "Writes current inclusion status for the given job and candidate patient UID")
    @PostMapping("/relevance")
    public @ResponseBody
    Boolean writeCohortRelevance(Authentication authentication,
                                 @RequestParam(name = "job_uid") UUID jobUID,
                                 @RequestParam(name = "patient_uid") String patientUID,
                                 @RequestParam(name = "judgement") CandidateInclusion judgement) {
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

    @Operation(summary = "Gets match judgement (if present), or algorithmicly determined judgement for a given list " +
            "of evidence UIDs associated with a given job and criterion (node) UID")
    @GetMapping("/evidence_relevance")
    public @ResponseBody
    Map<String, CriterionJudgement> getEvidenceRelevance(Authentication authentication,
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

    @Operation(summary = "Writes match judgement for a given evidence UID associated with a given job and criterion (node) UID")
    @PostMapping("/evidence_relevance")
    public @ResponseBody
    Boolean writeEvidenceRelevance(Authentication authentication,
                                   @RequestParam(name = "job_uid") UUID jobUID,
                                   @RequestParam(name = "node_uid") UUID nodeUID,
                                   @RequestParam(name = "evidence_uid") String evidenceUID,
                                   @RequestParam(name = "judgement") CriterionJudgement judgement) {
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

    @Operation(summary = "Gets match judgement (if present), or algorithmicly determined judgement for all nodes " +
            "of the Criterion associated with the given job UID")
    @GetMapping("/criterion_match_status")
    public @ResponseBody
    Map<String, CriterionInfo> getCriterionMatchStatus(Authentication authentication,
                                                       @RequestParam(name = "job_uid") UUID jobUID,
                                                       @RequestParam(name = "person_uid") String personUID) {
        try {
            return storage.getCriterionMatchStatus(authentication, jobUID, personUID);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }

    @Operation(summary = "Writes criterion match judgement for the given job, person, and criterion (node) UID")
    @PostMapping("/criterion_match_status")
    public @ResponseBody
    Map<String, CriterionInfo> setCriterionMatchStatus(Authentication authentication,
                                                       @RequestParam(name = "job_uid") UUID jobUID,
                                                       @RequestParam(name = "node_uid") UUID nodeUID,
                                                       @RequestParam(name = "person_uid") String personUID,
                                                       @RequestBody CriterionInfo judgement) {
        try {
            return storage.setCriterionMatchStatus(authentication, jobUID, nodeUID, personUID, judgement);
        } catch (Throwable e) {
            // TODO log the Exception
            throw new RuntimeException("Error occurred on cohort retrieval result retrieve");
        }
    }


    @Operation(summary = "Gets the revision of a criterion associated with a given job UID. ",
            description = "Note that for projects, the associated method under /_projects should be called instead using project UID as a parameter " +
                    "as the criterion associated with a prior job may be different from the latest revision being edited")
    @GetMapping("/criterion")
    public @ResponseBody
    Criterion getJobCriterion(Authentication authentication, @RequestParam(name = "job_uid") UUID uid) {
        try {
            return storage.getJobCriterion(authentication, uid);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }

    @Operation(summary = "Gets the FHIR resources associated with a given set of evidence UIDs")
    @GetMapping("/evidencebyuid")
    Map<String, DomainResource> getEvidenceByUID(@RequestParam(name = "evidenceUID") String... evidenceUIDs) {
        Map<String, DomainResource> ret = new HashMap<>();
        for (String evidenceUID : evidenceUIDs) {
            ret.put(evidenceUID, evidenceProvider.getEvidenceForUID(evidenceUID));
        }
        return ret;
    }
}
