package org.ohnlp.cat.executors;

import org.ohnlp.cat.ApplicationConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class JobExecutorManager {
    private final JobExecutor executor;
    @Autowired
    public JobExecutorManager(ApplicationConfiguration config, ApplicationContext appContext) throws ClassNotFoundException {
        String jobExecutorClazz = config.getJobExecutorClass();
        executor = (JobExecutor) appContext.getBean(Class.forName(jobExecutorClazz));
    }

    public JobExecutor getExecutor() {
        return executor;
    }
}
