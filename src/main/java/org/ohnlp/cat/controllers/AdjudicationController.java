package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.ohnlp.cat.api.adjudication.CohortAdjudicationStatus;
import org.ohnlp.cat.api.adjudication.PatientAdjudicationStatus;
import org.ohnlp.cat.api.cohorts.CandidateInclusion;
import org.ohnlp.cat.api.criteria.CriterionJudgement;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@Tag(name = "Adjudication Controller", description = "Operations relating to adjudication across different abstractors")
@Controller
@RequestMapping("/_adjudication")
public class AdjudicationController {

    private final JDBCBackedStorage storage;

    @Autowired
    public AdjudicationController(JDBCBackedStorage storage) {
        this.storage = storage;
    }

    @Operation(summary = "Gets adjudication/inclusion status of patients across all abstractors")
    @GetMapping("/cohort")
    public @ResponseBody
    Map<String, CohortAdjudicationStatus> getAdjudicationConflictsByCohort(Authentication authentication,
                                                                           @RequestParam("job_uid") UUID jobUID) {
        try {
            return storage.getCohortAdjudicationState(authentication, jobUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on cohort adjudication status retrieval");
        }
    }

    @Operation(summary = "Sets a tiebreaker/override adjudication for a given job and cohort candidate")
    @PostMapping("/cohort")
    public @ResponseBody
    Map<String, CohortAdjudicationStatus> setAdjudicatorOverride(Authentication authentication,
                                                                 @RequestParam("job_uid") UUID jobUID,
                                                                 @RequestParam("person_uid") String personUID,
                                                                 @RequestParam("state") CandidateInclusion status) {
        try {
            return storage.setAdjudicationOverrideCohort(authentication, jobUID, personUID, status);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on cohort adjudication status retrieval");
        }
    }

    @Operation(summary = "Gets adjudication/inclusion status for cohort criteria by job and person UID")
    @GetMapping("/person")
    public @ResponseBody
    Map<String, PatientAdjudicationStatus> getAdjudicationConflictsByPersonCriteria(Authentication authentication,
                                                                                    @RequestParam("job_uid") UUID jobUID,
                                                                                    @RequestParam("person_uid") String personUID) {
        try {
            return storage.getCriteriaAdjudicationState(authentication, jobUID, personUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on cohort adjudication status retrieval");
        }
    }
    @Operation(summary = "Sets a tiebreaker/override adjudication for a given job, person, and node uid")
    @PostMapping("/person")
    public @ResponseBody
    Map<String, PatientAdjudicationStatus> setAdjudicatorOverride(Authentication authentication,
                                                                 @RequestParam("job_uid") UUID jobUID,
                                                                 @RequestParam("person_uid") String personUID,
                                                                 @RequestParam("node_uid") UUID nodeUID,
                                                                 @RequestParam("state") CriterionJudgement status) {
        try {
            return storage.setAdjudicationOverrideCriterion(authentication, jobUID, nodeUID, personUID, status);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on cohort adjudication status retrieval");
        }
    }

}
