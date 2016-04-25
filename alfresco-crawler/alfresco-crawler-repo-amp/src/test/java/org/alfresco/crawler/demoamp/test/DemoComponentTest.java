package org.alfresco.crawler.demoamp.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.alfresco.crawler.demoamp.DemoComponent;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.content.MimetypeMap;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.transaction.RetryingTransactionHelper.RetryingTransactionCallback;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.model.FileFolderService;
import org.alfresco.service.cmr.model.FileInfo;
import org.alfresco.service.cmr.repository.ContentService;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.search.SearchService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.transaction.TransactionService;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.tradeshift.test.remote.Remote;
import com.tradeshift.test.remote.RemoteTestRunner;

/**
 * A simple class demonstrating how to run out-of-container tests 
 * loading Alfresco application context.
 * 
 * This class uses the RemoteTestRunner to try and connect to 
 * localhost:4578 and send the test name and method to be executed on 
 * a running Alfresco. One or more hostnames can be configured in the @Remote
 * annotation.
 * 
 * If there is no available remote server to run the test, it falls 
 * back on local running of JUnits.
 * 
 * For proper functioning the test class file must match exactly 
 * the one deployed in the webapp (either via JRebel or static deployment)
 * otherwise "incompatible magic value XXXXX" class error loading issues will arise.  
 * 
 * @author Gabriele Columbro 
 * @author Maurizio Pillitu
 *
 */
@RunWith(RemoteTestRunner.class)
@Remote(runnerClass=SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:alfresco/application-context.xml")
public class DemoComponentTest {
    
    private static final int NUMBER_OF_TESTING_NODES =10;
    
    private static final String ADMIN_USER_NAME = "admin";

    static Logger log = Logger.getLogger(DemoComponentTest.class);

    @Autowired
    protected DemoComponent demoComponent;
    
    @Autowired
    @Qualifier("NodeService")
    protected NodeService nodeService;
    
    @Test
    public void testWiring() {
        assertNotNull(demoComponent);
    }
    
    @Autowired
    @Qualifier("FileFolderService")
    private FileFolderService ffs;
    
    @Autowired
    @Qualifier("SearchService")
    private SearchService searchService;
 
    @Autowired
    @Qualifier("ServiceRegistry")
    private  ServiceRegistry serviceRegistry;
    
    private String testFolderName;
    
    private NodeRef testFolderNodeRef;
    
    private ArrayList<NodeRef> listOfNodeRef;
    
    @Before
    public  void before() {    
            AuthenticationUtil.setFullyAuthenticatedUser(ADMIN_USER_NAME);
            NodeRef companyHome = demoComponent.getCompanyHome();
            // create a test folder for this run
            testFolderName = "TestVersion" +  System.currentTimeMillis();
            testFolderNodeRef = ffs.create(companyHome, testFolderName, ContentModel.TYPE_FOLDER).getNodeRef();
            
            // create children and populate with versioned nodes
            createAndPopulate(testFolderNodeRef);
    }
    
    private void createAndPopulate(NodeRef testFolderNodeRef)
    {
        listOfNodeRef = new ArrayList<NodeRef>(NUMBER_OF_TESTING_NODES);
        createFirstGeneration(testFolderNodeRef, listOfNodeRef);

    }
    
    private void createFirstGeneration(NodeRef testFolderNodeRef, List<NodeRef> listOfNodeRef)
    {
        final NodeRef finalTestFolderNodeRef = testFolderNodeRef;
        final NodeService finalNodeService = nodeService;
        final List<NodeRef> finalListOfNodeRef = listOfNodeRef;
        // use TransactionWork to wrap service calls in a user transaction
        TransactionService transactionService = serviceRegistry.getTransactionService();
        RetryingTransactionCallback<Object> firstGenWork = new RetryingTransactionCallback<Object>()
        {
            public Object execute() throws Exception
            {
                ContentService contentService = serviceRegistry.getContentService();
                for(int i = 0; i < NUMBER_OF_TESTING_NODES; i++)
                {
                    FileInfo fi = ffs.create(finalTestFolderNodeRef, "TESTNODE" + System.currentTimeMillis() + i, ContentModel.TYPE_CONTENT);
                    // adding versionable aspect
                    
                    finalListOfNodeRef.add(fi.getNodeRef());
 
                    //Add some content
                    //
                    // write some content to new node
                    //
                    ContentWriter writer = contentService.getWriter(fi.getNodeRef(), ContentModel.PROP_CONTENT, true);
                    writer.setMimetype(MimetypeMap.MIMETYPE_TEXT_PLAIN);
                    writer.setEncoding("UTF-8");
                    String text = "The quick brown fox jumps over the lazy dog" + i;
                    writer.putContent(text);
                    //add vesionable aspect
                    HashMap<QName, Serializable> props = new HashMap<QName, Serializable>();
                    props.put(ContentModel.PROP_INITIAL_VERSION, false);
                    finalNodeService.addAspect(fi.getNodeRef(),ContentModel.ASPECT_VERSIONABLE, props);
                }
                return null;
            }
        };
        transactionService.getRetryingTransactionHelper().doInTransaction(firstGenWork);
    }
    

    
    
    @Test
    public void testGetCompanyHome() {
    	AuthenticationUtil.setFullyAuthenticatedUser(ADMIN_USER_NAME);
        NodeRef companyHome = demoComponent.getCompanyHome();
        assertNotNull(companyHome);
        String companyHomeName = (String) nodeService.getProperty(companyHome, ContentModel.PROP_NAME);
        assertNotNull(companyHomeName);
        assertEquals("Company Home", companyHomeName);
    }
    
    @Test
    public void testChildNodesCount() {
    	AuthenticationUtil.setFullyAuthenticatedUser(ADMIN_USER_NAME);
        NodeRef companyHome = demoComponent.getCompanyHome();
        int childNodeCount = demoComponent.childNodesCount(companyHome);
        assertNotNull(childNodeCount);
        // There are 8 folders by default under Company Home
        assertEquals(8, childNodeCount);
    }

}
