/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.scale7.cassandra.pelops.spring;

import java.util.Arrays;

import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

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
 *
 * <p>Note: the use of the class required the *optional* spring dependency.</p>
 */
public class CommonsBackedPoolFactoryBean
        implements FactoryBean<IThriftPool>, InitializingBean, DisposableBean {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPoolFactoryBean.class);

    private Cluster cluster;
    private String keyspace;
    private CommonsBackedPool.Policy policy;
    private OperandPolicy operandPolicy;
    private CommonsBackedPool.INodeSelectionStrategy nodeSelectionStrategy;
    private CommonsBackedPool.INodeSuspensionStrategy nodeSuspensionStrategy;
    private CommonsBackedPool.IConnectionValidator connectionValidator;

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

        logger.info("Initializing Pelops pool keyspace {} for nodes {}", getKeyspace(), Arrays.toString(getCluster().getNodes()));

        this.thriftPool = new CommonsBackedPool(
                getCluster(), getKeyspace(), getPolicy(), getOperandPolicy(), getNodeSelectionStrategy(),
                getNodeSuspensionStrategy(), getConnectionValidator()
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

    public CommonsBackedPool.Policy getPolicy() {
        return policy;
    }

    public void setPolicy(CommonsBackedPool.Policy policy) {
        this.policy = policy;
    }

    public OperandPolicy getOperandPolicy() {
        return operandPolicy;
    }

    public void setOperandPolicy(OperandPolicy operandPolicy) {
        this.operandPolicy = operandPolicy;
    }

    public CommonsBackedPool.INodeSelectionStrategy getNodeSelectionStrategy() {
        return nodeSelectionStrategy;
    }

    public void setNodeSelectionStrategy(CommonsBackedPool.INodeSelectionStrategy nodeSelectionStrategy) {
        this.nodeSelectionStrategy = nodeSelectionStrategy;
    }

    public CommonsBackedPool.INodeSuspensionStrategy getNodeSuspensionStrategy() {
        return nodeSuspensionStrategy;
    }

    public void setNodeSuspensionStrategy(CommonsBackedPool.INodeSuspensionStrategy nodeSuspensionStrategy) {
        this.nodeSuspensionStrategy = nodeSuspensionStrategy;
    }

    public CommonsBackedPool.IConnectionValidator getConnectionValidator() {
        return connectionValidator;
    }

    public void setConnectionValidator(CommonsBackedPool.IConnectionValidator connectionValidator) {
        this.connectionValidator = connectionValidator;
    }
}
