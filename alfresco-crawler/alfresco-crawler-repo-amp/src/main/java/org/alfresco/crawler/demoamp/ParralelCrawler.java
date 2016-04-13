package org.alfresco.crawler.demoamp;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import java.util.Date;

import org.alfresco.repo.lock.JobLockService;
import org.alfresco.repo.lock.JobLockService.JobLockRefreshCallback;
import org.alfresco.repo.lock.LockAcquisitionException;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.security.authentication.AuthenticationUtil.RunAsWork;
import org.alfresco.service.cmr.search.SearchService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.transaction.TransactionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.alfresco.util.PropertyCheck;
import org.alfresco.util.VmShutdownListener.VmShutdownException;

/**
 * Call an action on every node found by a configured query
 * 
 * @author Philippe
 */
public class ParralelCrawler implements ApplicationEventPublisherAware
{
    private long LOCK_TIME_TO_LIVE = 10000;
    private long LOCK_REFRESH_TIME = 5000;
    private static Log logger = LogFactory.getLog(ParralelCrawler.class);
    private ApplicationEventPublisher applicationEventPublisher;
    private SearchService searchService;
    private TransactionService transactionService;
    private Date startDate;
    private Date endDate;
    private Boolean isRunning;
    private JobLockService jobLockService;

    public void setJobLockService(JobLockService jobLockService)
    {
        this.jobLockService = jobLockService;
    }

    /**
     * Perform basic checks to ensure that the necessary dependencies were injected.
     */
    private void checkProperties()
    {

        PropertyCheck.mandatory(this, "transactionService", transactionService);
        PropertyCheck.mandatory(this, "searchService", searchService);
    }

    /**
     * @param queryService used to retrieve older versions
     */
    public void setSearchService(SearchService searchService)
    {
        this.searchService = searchService;
    }

    /**
     * @param transactionService the component to ensure proper transactional wrapping
     */
    public void setTransactionService(TransactionService transactionService)
    {
        this.transactionService = transactionService;
    }

    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher)
    {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public void execute()
    {
        checkProperties();

        // Bypass if the system is in read-only mode
        if (transactionService.isReadOnly())
        {
            logger.debug("Version store cleaner bypassed; the system is read-only.");
            return;
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Starting version store cleanup.");
        }

        try
        {
            QName lockQName = QName.createQName("pc", "crawl");
            String lockToken = jobLockService.getLock(lockQName, LOCK_TIME_TO_LIVE, 0, 1);
            TrackerJobLockRefreshCallback callback = new TrackerJobLockRefreshCallback();

            jobLockService.refreshLock(lockToken, lockQName, LOCK_REFRESH_TIME, callback);
            try
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("PUSH: job lock held");
                }
                
                try
                {
                    AuthenticationUtil.runAsSystem(new RunAsWork<Void>()
                    {
                        public Void doWork() throws Exception
                        {
                            executeInternal();
                            return null;
                        }
                    });
                }
                catch (Exception e)
                {
                        // This is where push comms failure is logged on the first time
                        logger.error("PUSH: unable to push:" + e.getMessage());
                }
            }
            finally
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("PUSH: job finished");
                }
                
                // Release the locks on the job and stop refreshing
                callback.isActive = false;
                jobLockService.releaseLock(lockToken, lockQName);
            }
        }
        catch (LockAcquisitionException e)
        {
            if (logger.isDebugEnabled())
            {
                // probably already running - or repo could be read only
                logger.debug("ParralelCrawler: unable to obtain job lock - probably already running");
            }
        }

    }

    protected void executeInternal()
    {

    }

    private class TrackerJobLockRefreshCallback implements JobLockRefreshCallback
    {
        public boolean isActive = true;

        @Override
        public boolean isActive()
        {
            return isActive;
        }

        @Override
        public void lockReleased()
        {
            if (logger.isTraceEnabled())
            {
                logger.trace("lock released");
            }
        }
    };

}
