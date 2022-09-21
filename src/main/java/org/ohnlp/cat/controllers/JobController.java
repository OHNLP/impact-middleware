package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.ohnlp.cat.api.jobs.Job;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Tag(name="Job Controller", description="Operations relating to Job Creation, Management, and Status")
@Controller
@RequestMapping("/_jobs")
public class JobController {
    // TODO handle backends...
    private final JDBCBackedStorage storage;

    @Autowired
    public JobController(JDBCBackedStorage storage) {
        this.storage = storage;
    }

    @Operation(summary="Gets a listing of jobs associated to the calling user, sorted by date in descending order")
    @GetMapping("/user")
    public @ResponseBody
    List<Job> getJobsForUser(Authentication authentication) {
        try {
            return storage.getJobsForUser(authentication);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
    }

    @Operation(summary="Gets a listing of jobs associated with the given project UID, sorted by date in descending order")
    @GetMapping("/project")
    public @ResponseBody
    List<Job> getJobsByProject(Authentication authentication, @RequestParam("project_uid") UUID projectUID) {
        try {
            return storage.getJobsForProject(authentication, projectUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
    }


    @Operation(summary="Queues a new job for the given project UID")
    @PostMapping("/create")
    public @ResponseBody
    Job createJob(Authentication authentication, @RequestParam("project_uid") UUID projectUID) {
        Job jobInfo;
        try {
            jobInfo = storage.runJob(authentication, projectUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
        return jobInfo;
    }

    @Operation(summary="Cancels the job associated with the given job UID")
    @PostMapping("/cancel")
    public @ResponseBody
    Boolean cancelJob(Authentication authentication, @RequestParam("job_uid") UUID jobUID) {
        try {
            if (!storage.cancelJobRecord(authentication, jobUID)) {
                return false;
            }
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job cancel");
        }
        return false;
    }

    @Operation(summary="Archives the job associated with the given job UID")
    @DeleteMapping("/")
    public @ResponseBody
    Boolean archiveJob(Authentication authentication, @RequestParam("job_uid") UUID jobUID) {
        try {
            return storage.archiveJobRecord(authentication, jobUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
    }

}
