package org.datanucleus.test;

import java.sql.Connection;

import java.sql.ResultSet;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;
import javax.jdo.annotations.SequenceStrategy;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.metadata.JDOMetadata;

import org.datanucleus.util.NucleusLogger;
import org.junit.Test;
import static org.junit.Assert.*;

public class SimpleTest
{
    @Test
    public void testSimple()
    {
        NucleusLogger.GENERAL.info(">> test START");
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("MyTest");
        

        
        JDOMetadata md = pmf.newMetadata();
        md.newPackageMetadata("test").newSequenceMetadata("userseq", SequenceStrategy.NONCONTIGUOUS).
            setDatastoreSequence("users_seq").setAllocationSize(10);
        pmf.registerMetadata(md);
        
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();
            Sequence seq = pm.getSequence("userseq");
            long l = seq.nextValue();
            JDOConnection jdo = pm.getDataStoreConnection();
            Connection con = (Connection) jdo.getNativeConnection();
            
            ResultSet rs = con.createStatement().executeQuery("select NEXT_VAL from SEQUENCE_TABLE where SEQUENCE_NAME = 'users_seq'");
            rs.next();
            long current = rs.getLong(1);
            rs.close();
            jdo.close();
            if( current != 10L) {
                fail("Current seq = " + current);
            }
            
            tx.commit();
        }
        catch (Throwable thr)
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

        pmf.close();
        NucleusLogger.GENERAL.info(">> test END");
    }
}
