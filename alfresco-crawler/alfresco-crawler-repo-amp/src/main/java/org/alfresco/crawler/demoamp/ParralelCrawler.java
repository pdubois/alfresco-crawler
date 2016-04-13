package org.alfresco.crawler.demoamp;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import java.util.Date;

import org.alfresco.service.cmr.search.SearchService;
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
    private static Log logger = LogFactory.getLog(ParralelCrawler.class);
    private ApplicationEventPublisher applicationEventPublisher;
    private SearchService searchService;
    private TransactionService transactionService;
    private Date startDate;
    private Date endDate;
    private Boolean isRunning;
    
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
        refreshLock();
        // Repeat attempts six times waiting 10 minutes between
        startDate = new Date();
        isRunning = true;
        try
        {

            executeInternal();
        }
        catch (VmShutdownException e)
        {
            // Aborted
            if (logger.isDebugEnabled())
            {
                logger.debug("   Content store cleanup aborted.");
            }
        }
        finally
        {
            endDate = new Date();
            isRunning = false;
            releaseLock();
        }

    }
    
    protected void executeInternal()
    {
        
    }
    
    protected void refreshLock()
    {
        
    }
    
    protected void releaseLock()
    {
        
    }
    
}
