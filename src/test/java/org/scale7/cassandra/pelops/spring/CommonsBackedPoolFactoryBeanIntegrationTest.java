package org.scale7.cassandra.pelops.spring;

import org.apache.cassandra.thrift.CfDef;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

import java.util.ArrayList;
import java.util.Arrays;

import static junit.framework.Assert.*;

/**
 * Tests the {@link CommonsBackedPoolFactoryBean} class.
 *
 * <p>Note: it uses the debugging pool option to avoid attempting to connect to a Cassandra instance.
 */
public class CommonsBackedPoolFactoryBeanIntegrationTest extends AbstractIntegrationTest {
	@BeforeClass
	public static void setup() throws Exception {
		setup(new ArrayList<CfDef>());
	}

    /**
     * Tests the factory bean works as expected when no operand or pool policy are specified.
     * @throws Exception if an error occurs
     */
    @Test
    public void testAfterPropertiesSetSimpleUseCase() throws Exception {
        CommonsBackedPoolFactoryBean factoryBean = new CommonsBackedPoolFactoryBean();
        factoryBean.setCluster(AbstractIntegrationTest.cluster);
        factoryBean.setKeyspace(AbstractIntegrationTest.KEYSPACE);

        assertNull("The factory should not have created the pool at this point", factoryBean.getObject());

        try {
            factoryBean.afterPropertiesSet();

            assertNotNull("The factory didn't initialize the pool", factoryBean.getObject());
            assertNotNull("The factory didn't initialize a default operand policy instance", factoryBean.getOperandPolicy());
            assertNotNull("The factory didn't initialize a default pool config instance", factoryBean.getConfig());
            assertNotNull("The factory didn't initialize a default node selection policy instance", factoryBean.getNodeSelectionPolicy());
        } finally {
            factoryBean.destroy();
        }
    }

    /**
     * Tests the factory bean works as expected when operand or pool policy instances are provided.
     * @throws Exception if an error occurs
     */
    @Test
    public void testAfterProperties() throws Exception {
        OperandPolicy operandPolicy = new OperandPolicy();
        CommonsBackedPool.LeastLoadedNodeSelectionPolicy nodeSelectionPolicy = new CommonsBackedPool.LeastLoadedNodeSelectionPolicy();
        CommonsBackedPool.Config config = new CommonsBackedPool.Config();

        CommonsBackedPoolFactoryBean factoryBean = new CommonsBackedPoolFactoryBean();
        factoryBean.setCluster(AbstractIntegrationTest.cluster);
        factoryBean.setKeyspace(AbstractIntegrationTest.KEYSPACE);
        factoryBean.setConfig(config);
        factoryBean.setOperandPolicy(operandPolicy);
        factoryBean.setNodeSelectionPolicy(nodeSelectionPolicy);

        assertNull("The factory should not have created the pool at this point", factoryBean.getObject());

        try {
            factoryBean.afterPropertiesSet();

            assertNotNull("The factory didn't initialize the pool", factoryBean.getObject());
            assertTrue("The factory didn't use the provided operand policy instance", operandPolicy == factoryBean.getOperandPolicy());
            assertTrue("The factory didn't use the provided config instance", config == factoryBean.getConfig());
            assertTrue("The factory didn't use the provided node selection instance", nodeSelectionPolicy == factoryBean.getNodeSelectionPolicy());
        } finally {
            factoryBean.destroy();
        }
    }

    /**
     * Test to ensure that the required keyspace property is validated.
     * @throws Exception if an error occurs
     */
    @Test
    public void testValidationKeyspace() throws Exception {
        CommonsBackedPoolFactoryBean factoryBean = new CommonsBackedPoolFactoryBean();
        factoryBean.setCluster(AbstractIntegrationTest.cluster);
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
        CommonsBackedPoolFactoryBean factoryBean = new CommonsBackedPoolFactoryBean();
        factoryBean.setKeyspace("keyspace");
        try {
            factoryBean.afterPropertiesSet();
            fail("The factory bean should have failed with a missing cluster property");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
