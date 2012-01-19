package org.scale7.cassandra.pelops.spring;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.cassandra.thrift.CfDef;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.LeastLoadedNodeSelectionStrategy;
import org.scale7.cassandra.pelops.pool.NoOpConnectionValidator;
import org.scale7.cassandra.pelops.pool.NoOpNodeSuspensionStrategy;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

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

            CommonsBackedPool pool = (CommonsBackedPool) factoryBean.getObject();
            assertNotNull("The factory didn't initialize the pool", pool);
            assertNotNull("The factory didn't initialize a default operand policy instance", pool.getOperandPolicy());
            assertNotNull("The factory didn't initialize a default pool config instance", pool.getPolicy());
            assertNotNull("The factory didn't initialize a default node selection policy instance", pool.getNodeSelectionStrategy());
            assertNotNull("The factory didn't initialize a default node suspension policy instance", pool.getNodeSuspensionStrategy());
            assertNotNull("The factory didn't initialize a default connection validator policy instance", pool.getConnectionValidator());
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
        CommonsBackedPool.Policy policy = new CommonsBackedPool.Policy();
        LeastLoadedNodeSelectionStrategy nodeSelectionStrategy = new LeastLoadedNodeSelectionStrategy();
        NoOpNodeSuspensionStrategy nodeSuspensionStrategy = new NoOpNodeSuspensionStrategy();
        NoOpConnectionValidator connectionValidator = new NoOpConnectionValidator();

        CommonsBackedPoolFactoryBean factoryBean = new CommonsBackedPoolFactoryBean();
        factoryBean.setCluster(AbstractIntegrationTest.cluster);
        factoryBean.setKeyspace(AbstractIntegrationTest.KEYSPACE);
        factoryBean.setPolicy(policy);
        factoryBean.setOperandPolicy(operandPolicy);
        factoryBean.setNodeSelectionStrategy(nodeSelectionStrategy);
        factoryBean.setNodeSuspensionStrategy(nodeSuspensionStrategy);
        factoryBean.setConnectionValidator(connectionValidator);

        assertNull("The factory should not have created the pool at this point", factoryBean.getObject());

        try {
            factoryBean.afterPropertiesSet();

            CommonsBackedPool pool = (CommonsBackedPool) factoryBean.getObject();
            assertNotNull("The factory didn't initialize the pool", pool);
            assertTrue("The factory didn't use the provided operand policy instance", operandPolicy == pool.getOperandPolicy());
            assertTrue("The factory didn't use the provided config instance", policy == pool.getPolicy());
            assertTrue("The factory didn't use the provided config instance", cluster == pool.getCluster());
            assertTrue("The factory didn't use the provided node selection instance", nodeSelectionStrategy == pool.getNodeSelectionStrategy());
            assertTrue("The factory didn't use the provided node suspension instance", nodeSuspensionStrategy == pool.getNodeSuspensionStrategy());
            assertTrue("The factory didn't use the provided connection validator instance", connectionValidator == pool.getConnectionValidator());
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
