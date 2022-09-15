package org.ohnlp.cat.controllers;

import org.ohnlp.cat.dto.JobInfoDTO;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Controller
@RequestMapping("/_jobs")
public class JobController {
    // TODO handle backends...
    private final JDBCBackedStorage storage;

    @Autowired
    public JobController(JDBCBackedStorage storage) {
        this.storage = storage;
    }

    @GetMapping("/")
    List<JobInfoDTO> getJobsForUser(Authentication authentication) {
        try {
            return storage.getJobsForUser(authentication);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
    }

    @GetMapping("/for_project")
    List<JobInfoDTO> getJobsByProject(Authentication authentication, @RequestParam("project_uid") UUID projectUID) {
        try {
            return storage.getJobsForProject(authentication, projectUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
    }


    @PostMapping("/create")
    public @ResponseBody
    JobInfoDTO createJob(Authentication authentication, @RequestParam("project_uid") UUID projectUID) {
        JobInfoDTO jobInfo;
        try {
            jobInfo = storage.createJobRecord(authentication, projectUID);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
        // TODO actually trigger job (abstractified launcher for beam script)
        return jobInfo;
    }

    @PostMapping("/cancel")
    public @ResponseBody
    Boolean cancelJob(Authentication authentication, @RequestParam("job_uid") UUID jobUID) {
        try {
            if (!storage.cancelJobRecord(authentication, jobUID)) {
                return false;
            }
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
        return false; // TODO actually cancel the job instead of just creating record
    }

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
