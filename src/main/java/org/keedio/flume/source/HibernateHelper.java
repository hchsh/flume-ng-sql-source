package org.keedio.flume.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public class HibernateHelper {

    private static final Logger LOG = LoggerFactory
            .getLogger(HibernateHelper.class);

    private static SessionFactory factory;
    private Session session;
    private ServiceRegistry serviceRegistry;
    private Configuration config;
    private SQLSourceHelper sqlSourceHelper;
    private int termNumbers = 1;

    /**
     * Constructor to initialize hibernate configuration parameters
     *
     * @param sqlSourceHelper Contains the configuration parameters from flume config file
     */
    public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

        this.sqlSourceHelper = sqlSourceHelper;
        Context context = sqlSourceHelper.getContext();

        /* check for mandatory propertis */
        sqlSourceHelper.checkMandatoryProperties();

        Map<String, String> hibernateProperties = context.getSubProperties("hibernate.");
        Iterator<Map.Entry<String, String>> it = hibernateProperties.entrySet().iterator();

        config = new Configuration();
        Map.Entry<String, String> e;

        while (it.hasNext()) {
            e = it.next();
            config.setProperty("hibernate." + e.getKey(), e.getValue());
        }

    }

    /**
     * Connect to database using hibernate
     */
    public void establishSession() {

        LOG.info("Opening hibernate session");

        serviceRegistry = new StandardServiceRegistryBuilder()
                .applySettings(config.getProperties()).build();
        factory = config.buildSessionFactory(serviceRegistry);
        session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);

        session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
    }

    /**
     * Close database connection
     */
    public void closeSession() {

        LOG.info("Closing hibernate session");

        session.close();
        factory.close();
    }

    /**
     * Execute the selection query in the database
     *
     * @return The query result. Each Object is a cell content. <p>
     * The cell contents use database types (date,int,string...),
     * keep in mind in case of future conversions/castings.
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> executeQuery() throws InterruptedException {

        List<List<Object>> rowsList = new ArrayList<List<Object>>();
        Query query;
        String currentTime = "";
        if (!session.isConnected()) {
            resetConnection();
        }

        if (sqlSourceHelper.isCustomQuerySet()) {
            LOG.warn("This Plugin is altered by hcc.Because of removing the MaxRows ,please make the batch not too " +
                    "big ...");
            List<List<Object>> currentTimeList =
                    session.createSQLQuery("select unix_timestamp(now())").setResultTransformer(Transformers.TO_LIST).list();
            currentTime = currentTimeList.get(0).get(0).toString().substring(0, 10);
            currentTime = Integer.toString((Integer.valueOf(currentTime) - 10));
            currentTime = min(sqlSourceHelper.getCurrentIndex(), currentTime, 600);
            query = session.createSQLQuery(sqlSourceHelper.buildQuery(currentTime));

//            if (sqlSourceHelper.getMaxRows() != 0) {
//                LOG.warn("plugin altered by hcc. please set max.rows = 0 !!! ");
//            }
        } else {
            LOG.error("plugins altered by hcc. please set CustomQuery!!! Exception thrown, resetting connection.");
            resetConnection();
            return rowsList;
        }

        try {
            rowsList =
                    query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
        } catch (Exception e) {
            LOG.error("Exception thrown, resetting connection.", e);
            resetConnection();
        }

        if (!rowsList.isEmpty()) {
            sqlSourceHelper.setCurrentIndex(currentTime);
            termNumbers = 1;
        }

        return rowsList;
    }

    private void resetConnection() throws InterruptedException {
        if (session.isOpen()) {
            session.close();
            factory.close();
        } else {
            establishSession();
        }

    }

    private String min(String str1, String str2, int term) {
        Long l1 = Long.valueOf(str1);
        Long l2 = Long.valueOf(str2);
        if (l1 + term * termNumbers >= l2) {
            return l2.toString();
        } else {
            l1 += term * termNumbers;
            termNumbers++;
            return l1.toString();
        }
    }
}



