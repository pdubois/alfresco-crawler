package org.alfresco.crawler.demoamp;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.alfresco.repo.action.executer.ScriptActionExecuter;
import org.alfresco.repo.batch.BatchProcessor;
import org.alfresco.repo.batch.BatchProcessor.BatchProcessWorker;
import org.alfresco.repo.jscript.ScriptNode;
import org.alfresco.repo.jscript.ScriptUtils;
import org.alfresco.repo.lock.JobLockService;
import org.alfresco.repo.lock.JobLockService.JobLockRefreshCallback;
import org.alfresco.repo.nodelocator.NodeLocatorService;
import org.alfresco.repo.nodelocator.XPathNodeLocator;
import org.alfresco.repo.lock.LockAcquisitionException;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.security.authentication.AuthenticationUtil.RunAsWork;
import org.alfresco.repo.transaction.RetryingTransactionHelper.RetryingTransactionCallback;
import org.alfresco.service.cmr.action.Action;
import org.alfresco.service.cmr.action.ActionService;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.cmr.search.ResultSet;
import org.alfresco.service.cmr.search.SearchParameters;
import org.alfresco.service.cmr.search.SearchService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.transaction.TransactionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.alfresco.util.PropertyCheck;
import org.alfresco.util.VmShutdownListener;
import org.alfresco.util.VmShutdownListener.VmShutdownException;

/**
 * Call an action on every node found by a configured query
 * 
 * @author Philippe
 */
public class ParallelCrawler implements ApplicationEventPublisherAware
{
    private static VmShutdownListener vmShutdownListener = new VmShutdownListener("ParralelCrawler");
    private final static String baseScriptPath = "/app:company_home/app:dictionary/app:scripts/";
    private long LOCK_TIME_TO_LIVE = 10000;
    private long LOCK_REFRESH_TIME = 5000;
    private int bigPageLen = 50000;
    private static Log logger = LogFactory.getLog(ParallelCrawler.class);
    private ApplicationEventPublisher applicationEventPublisher;
    private SearchService searchService;
    private TransactionService transactionService;
    private Date startDate;
    private Date endDate;
    private Boolean isRunning;
    private JobLockService jobLockService;
    private String query;
    private String scriptName;
    private ActionService actionService;
    private NodeLocatorService nodeLocatorService;
    
    public void setBigPageLen(int bigPageLen)
    {
        this.bigPageLen = bigPageLen;
    }
    
    public void setNodeLocatorService(NodeLocatorService nodeLocatorService)
    {
        this.nodeLocatorService = nodeLocatorService;
    }


    public void setActionService(ActionService actionService)
    {
        this.actionService = actionService;
    }


    public void setScriptName(String scriptName)
    {
        this.scriptName = scriptName;
    }

    private int threadNumber = 2;
    
    

    public void setThreadNumber(int threadNumber)
    {
        this.threadNumber = threadNumber;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

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
        PropertyCheck.mandatory(this, "nodeLocatorService", nodeLocatorService);
        PropertyCheck.mandatory(this, "actionService", actionService);
        PropertyCheck.mandatory(this, "scriptName", scriptName);
        PropertyCheck.mandatory(this, "query", query);
        PropertyCheck.mandatory(this, "jobLockService", jobLockService);
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
                    logger.trace("ParralelCrawler: job lock held");
                }
                
                try
                {
                    AuthenticationUtil.runAsSystem(new RunAsWork<Void>()
                    {
                        public Void doWork() throws Exception
                        {
                            startDate = new Date();
                            isRunning = true;
                            executeInternal();
                            return null;
                        }
                    });
                }
                catch (Exception e)
                {
                        // This is where push comms failure is logged on the first time
                        logger.error("ParralelCrawler: unable to push:" + e.getMessage());
                }
                finally
                {
                    endDate = new Date();
                    isRunning = false;
                }
            }
            finally
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("ParralelCrawler: job finished");
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
        int startingElement = 0;
        int lot = 0;
        while (true)
        {
            lot++;
            // search by page
            final int staticStartingElement = startingElement;
            // execute in READ-WRITE txn
            RetryingTransactionCallback<Collection<NodeRef>> executeCallback = new RetryingTransactionCallback<Collection<NodeRef>>()
                {
                    public Collection<NodeRef> execute() throws Exception
                    {
                        StoreRef storeRef = new StoreRef(StoreRef.PROTOCOL_WORKSPACE, "SpacesStore");
                        // Get VersionableNodes
                        Collection<NodeRef> bigPageNodes = executeQuery(storeRef, searchService,
                                query, staticStartingElement, bigPageLen);
                        return bigPageNodes;
                    };
                };

            try
            {
                if (vmShutdownListener.isVmShuttingDown())
                {
                    throw new VmShutdownException();
                }
                Collection<NodeRef> nodesToCleaned = transactionService.getRetryingTransactionHelper().doInTransaction(
                        executeCallback, true);
                final BatchProcessor<NodeRef> groupProcessor = new BatchProcessor<NodeRef>("VersionCleaner", this.transactionService.getRetryingTransactionHelper(), nodesToCleaned, threadNumber,
                        5000,this.applicationEventPublisher, logger, 500);
                
                        
                class NodeVersionCleaner implements BatchProcessWorker<NodeRef>
                {
                    public String getIdentifier(NodeRef entry)
                    {
                        return entry.toString();
                    }

                    public void process(NodeRef currentNode) throws Throwable
                    {
                        final NodeRef fCurrentNode = currentNode;
                        AuthenticationUtil.runAsSystem(new RunAsWork<Void>()
                        {
                            public Void doWork() throws Exception
                            {
                                //execute the script action on the node
                                //see line 800 ActionServiceImplTest
                                String scriptPath = baseScriptPath + scriptName;
                                final Map<String, Serializable> params = new HashMap<>(1, 1.0f);
                                params.put(XPathNodeLocator.QUERY_KEY, scriptPath);
                                NodeRef script = nodeLocatorService.getNode(XPathNodeLocator.NAME, null, params);
                                Action action = actionService.createAction(ScriptActionExecuter.NAME);
                                action.setParameterValue(ScriptActionExecuter.PARAM_SCRIPTREF, script);
                                // Execute the action
                                actionService.executeAction(action, fCurrentNode);
                                return null;
                            }
                        });
                            
                        
                    }

                    @Override
                    public void beforeProcess() throws Throwable
                    {
                        // TODO Auto-generated method stub
                        
                    }

                    @Override
                    public void afterProcess() throws Throwable
                    {
                        // TODO Auto-generated method stub
                        
                    }
                }

                NodeVersionCleaner unitOfWork = new NodeVersionCleaner();

                groupProcessor.process(unitOfWork, true);

                startingElement += bigPageLen;
                // Done
                if (logger.isDebugEnabled())
                {
                    logger.debug("  Cleaning iteration:" + staticStartingElement);
                }
                if (nodesToCleaned.size() < bigPageLen)
                    break;
            }
            catch (VmShutdownException e)
            {
                // Aborted
                if (logger.isDebugEnabled())
                {
                    logger.debug("Version cleanup aborted.");
                }
                throw e;
            }
            catch (Throwable e)
            {
                e.printStackTrace();
                logger.warn("System shutting down during version cleaning at:" + staticStartingElement);
                break;
            }
        }
    }

    private Collection<NodeRef> executeQuery(
            StoreRef storeRef,
            SearchService searchService,
            String query,
            int startingElement,
            int pageLen)
    {

        SearchParameters sp = new SearchParameters();
        sp.addStore(storeRef);
        sp.setLanguage(SearchService.LANGUAGE_FTS_ALFRESCO);
        sp.setSkipCount(startingElement);
        // -1 unlimited result size
        sp.setMaxItems(-1);
        sp.setQuery(query);
        ResultSet results = searchService.query(sp);
        Collection<NodeRef> nodeToClean = new ArrayList<NodeRef>(pageLen);
        int i;
        for (i = startingElement; i < startingElement + pageLen; i++)
        {
            if (i - startingElement >= results.length())
                break;
            NodeRef nodeRef = results.getNodeRef(i - startingElement);
            nodeToClean.add(nodeRef);
        }
        results.close();
        return nodeToClean;
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
