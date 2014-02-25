package org.datanucleus.test;

import static org.junit.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import mydomain.model.Person;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.util.NucleusLogger;
import org.junit.Test;

public class SimpleTest
{
    @Test
    public void testSimple()
    {
        NucleusLogger.GENERAL.info(">> test START");
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("MyTest");
        

        
        
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();
            pm.makePersistent(new Person(5L, "foo"));
            tx.commit();
        }
        catch (Exception thr)
        {
            NucleusLogger.GENERAL.error(">> Exception thrown persisting data", thr);
            thr.printStackTrace();
            fail( "" + thr);
        }
        finally 
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
        
        pm = pmf.getPersistenceManager();
        pm.setProperty("datanucleus.cache.level2.type", "none");
        pm.close();
        
        pm = pmf.getPersistenceManager();
        ExecutionContext ec = ((JDOPersistenceManager)pm).getExecutionContext();
        if (!ec.hasIdentityInCache(ec.newObjectId(Person.class, 5L))) {
            fail("ID not in cache");
        }
        
        pmf.close();
        NucleusLogger.GENERAL.info(">> test END");
    }
}
