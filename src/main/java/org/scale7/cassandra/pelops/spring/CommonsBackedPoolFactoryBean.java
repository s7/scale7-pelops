package org.scale7.cassandra.pelops.spring;

import org.scale7.cassandra.pelops.*;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.pool.LeastLoadedNodeSelectionPolicy;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

import java.util.Arrays;

/**
 * <p>Used to initialize a Pelops pool that honors Spring's context life cycle.  Using this class ensures that the
 * dependency graph that's managed by Spring won't attempt to use Pelops when it isn't available.</p>
 *
 * To use it add the following to your context file:
 * <pre>
 * &lt;bean id="pelopsPool" class="org.scale7.cassandra.pelops.spring.CommonsBackedPoolFactoryBean"&gt;
 *      &lt;property name="cluster"&gt;
 *          &lt;bean class="org.scale7.cassandra.pelops.Cluster"&gt;
 *              &lt;constructor-arg index="0" type="java.lang.String" value="${digitalpigeon.cassandra.host}" /&gt;
 *              &lt;constructor-arg index="1" type="int" value="${digitalpigeon.cassandra.port}" /&gt;
 *          &lt;/bean&gt;
 *      &lt;/property&gt;
 *      &lt;property name="keyspace" value="keyspace" /&gt;
 * &lt;/bean&gt;
 * </pre>
 * <br/>
 * 
 * <p>NOTE: If you intend to use this class you'll need to bypass the static convenience methods on
 * {@link org.scale7.cassandra.pelops.Pelops}.</p>
 * <p>Inject the instance of {@link org.scale7.cassandra.pelops.pool.IThriftPool} created by this factory bean into your
 * application code and use it's method directly.</p>
 * For example:
 * <pre>
 * private IThriftPool pool;
 * </pre>
 * <pre>
 * public void doStuff() {
 *     Selector selector = pool.createSelector();
 *     ...
 * }
 * </pre>
 * <br/>
 */
public class CommonsBackedPoolFactoryBean
        implements FactoryBean<IThriftPool>, InitializingBean, DisposableBean {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPoolFactoryBean.class);

    private Cluster cluster;
    private String keyspace;
    private CommonsBackedPool.Config config;
    private CommonsBackedPool.INodeSelectionPolicy nodeSelectionPolicy;
    private OperandPolicy operandPolicy;

    private IThriftPool thriftPool;

    /**
     * {@inheritDoc}.
     * @return the pool
     */
    @Override
    public IThriftPool getObject() throws Exception {
        return thriftPool;
    }

    /**
     * {@inheritDoc}.
     * @return {@link IThriftPool}
     */
    @Override
    public Class<?> getObjectType() {
        return IThriftPool.class;
    }

    /**
     * {@inheritDoc}.
     * @return true
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * Initializes the Pelops pool.
     * @throws Exception if an error occurs
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(getCluster(), "The cluster property is required");
        Assert.notNull(getKeyspace(), "The keyspace property is required");

        logger.info("Initializing Pelops pool for nodes {}", Arrays.toString(getCluster().getNodes()));

        if (getConfig() == null) {
            logger.info("No configuration provided, using defaults");
            setConfig(new CommonsBackedPool.Config());
        }
        if (getNodeSelectionPolicy() == null) {
            logger.info("No node selection policy specified, using {}", LeastLoadedNodeSelectionPolicy.class.getName());
            setNodeSelectionPolicy(new LeastLoadedNodeSelectionPolicy());
        }
        if (getOperandPolicy() == null) {
            logger.info("No operand policy provided, using default");
            setOperandPolicy(new OperandPolicy());
        }

        this.thriftPool = new CommonsBackedPool(
                getCluster(), config, nodeSelectionPolicy, getOperandPolicy(), getKeyspace()
        );
    }

    /**
     * Shuts down the Pelops pool.
     * @throws Exception if an error occurs
     * @see {@link org.scale7.cassandra.pelops.pool.IThriftPool#shutdown()}
     */
    @Override
    public void destroy() throws Exception {
        this.thriftPool.shutdown();
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Required
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String getKeyspace() {
        return keyspace;
    }

    @Required
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public CommonsBackedPool.Config getConfig() {
        return config;
    }

    public void setConfig(CommonsBackedPool.Config config) {
        this.config = config;
    }

    public CommonsBackedPool.INodeSelectionPolicy getNodeSelectionPolicy() {
        return nodeSelectionPolicy;
    }

    public void setNodeSelectionPolicy(CommonsBackedPool.INodeSelectionPolicy nodeSelectionPolicy) {
        this.nodeSelectionPolicy = nodeSelectionPolicy;
    }

    public OperandPolicy getOperandPolicy() {
        return operandPolicy;
    }

    public void setOperandPolicy(OperandPolicy operandPolicy) {
        this.operandPolicy = operandPolicy;
    }
}
