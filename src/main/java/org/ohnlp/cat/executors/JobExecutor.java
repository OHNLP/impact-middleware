package org.ohnlp.cat.executors;

import org.ohnlp.cat.api.criteria.Criterion;

import java.util.UUID;

public interface JobExecutor {
    /**
     * Queues a job using a given jobUID. Depending on the specific executor used, execution may not be immediate
     * and may wait for resource start
     *
     * @param jobUID      The UUID of the Job
     * @param criterion   The criterion associated with the job
     * @param callbackURL The URL of the middleware server scheduling this job
     * @return The executor-local job ID (different from the middleware-tracking job ID) if provided by the executor,
     * null otherwise.
     * @throws Exception - If error occurs during the Job Scheduling Process
     */
    String startJob(UUID jobUID, Criterion criterion, String callbackURL) throws Exception;
}
