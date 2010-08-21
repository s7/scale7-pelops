package org.scale7.cassandra.pelops.spring;

import org.junit.Test;
import org.scale7.cassandra.pelops.*;

import static junit.framework.Assert.*;

/**
 * Tests the {@link org.scale7.cassandra.pelops.spring.PoolFactoryBean} class.
 *
 * <p>Note: it uses the debugging pool option to avoid attempting to connect to a Cassandra instance.
 */
public class PoolFactoryBeanUnitTest {
    /**
     * Tests the factory bean works as expected when no operand or pool policy are specified.
     * @throws Exception if an error occurs
     */
    @Test
    public void testAfterPropertiesSetSimpleUseCase() throws Exception {
        PoolFactoryBean factoryBean = new PoolFactoryBean();
        factoryBean.setCluster(new Cluster("node1", 1234));
        factoryBean.setKeyspace("keyspace");
        factoryBean.setDebugPoolRequired(true);

        assertNull("The factory should not have created the pool at this point", factoryBean.getObject());

        factoryBean.afterPropertiesSet();

        assertNotNull("The factory didn't initialize the pool", factoryBean.getObject());
        assertEquals("Wrong pool type created", DebuggingPool.class, factoryBean.getObject().getClass());
        assertNotNull("The factory didn't initialize a default operand policy instance", factoryBean.getOperandPolicy());
        assertNotNull("The factory didn't initialize a default pool policy instance", factoryBean.getPoolPolicy());
    }

    /**
     * Tests the factory bean works as expected when operand or pool policy instances are provided.
     * @throws Exception if an error occurs
     */
    @Test
    public void testAfterProperties() throws Exception {
        OperandPolicy operandPolicy = new OperandPolicy();
        CachePerNodePool.Policy pooPolicy = new CachePerNodePool.Policy();

        PoolFactoryBean factoryBean = new PoolFactoryBean();
        factoryBean.setCluster(new Cluster("node1", 1234));
        factoryBean.setKeyspace("keyspace");
        factoryBean.setDebugPoolRequired(true);
        factoryBean.setOperandPolicy(operandPolicy);
        factoryBean.setPoolPolicy(pooPolicy);

        assertNull("The factory should not have created the pool at this point", factoryBean.getObject());

        factoryBean.afterPropertiesSet();

        assertNotNull("The factory didn't initialize the pool", factoryBean.getObject());
        assertEquals("Wrong pool type created", DebuggingPool.class, factoryBean.getObject().getClass());
        assertTrue("The factory didn't use the provided operand policy instance", operandPolicy == factoryBean.getOperandPolicy());
        assertTrue("The factory didn't use the provided pool policy instance", pooPolicy == factoryBean.getPoolPolicy());
    }

    /**
     * Test to ensure that the required keyspace property is validated.
     * @throws Exception if an error occurs
     */
    @Test
    public void testValidationKeyspace() throws Exception {
        PoolFactoryBean factoryBean = new PoolFactoryBean();
        factoryBean.setCluster(new Cluster("node", 1234));
        try {
            factoryBean.afterPropertiesSet();
            fail("The factory bean should have failed with a missing keyspace property");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Test to ensure that the required clsuter property is validated.
     * @throws Exception if an error occurs
     */
    @Test
    public void testValidationCluster() throws Exception {
        PoolFactoryBean factoryBean = new PoolFactoryBean();
        factoryBean.setKeyspace("keyspace");
        try {
            factoryBean.afterPropertiesSet();
            fail("The factory bean should have failed with a missing cluster property");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
