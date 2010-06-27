package org.wyki.portability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A system that proxies the creation of certain objects and execution of some actions. 
 * This was originally created because if writing a Red5 application, when you wish to acquire a logger
 * for your specific application, as opposed to the logger belonging to the main server itself, it is necessary to
 * make the following call <code>Logger logger = Red5LoggerFactory.getLogger(clazz, "myApp");</code>. This proxy
 * makes it possible to have the wyki libraries log to the Red5 application, rather than the Red5 server.
 * 
 * @author dominicwilliams
 *
 */
public class SystemProxy {
	
	private static volatile ILoggerFactory loggerFactory = new ILoggerFactory() {
		
		@SuppressWarnings("unchecked")
		@Override
		public Logger getLogger(Class clazz) {
			return LoggerFactory.getLogger(clazz);
		}
	};
	
	/**
	 * Get a SLF4J <code>Logger</code> object (SLF4J provides a proxy for all the main Java logging systems used such that
	 * you can switch your application between systems simply by dropping in a jar proxy file). If the SLF4J <code>Logger</code>
	 * needs to be created in a special way, for example inside some J2EE application environment, then you can change the 
	 * default <code>ILoggerFactory</code> used to generate the logger using <code>setLoggerFactory</code>.
	 * @param clazz						A reference to the class performing the logging
	 * @return							The SLF4J <code>Logger</code> object
	 */
	@SuppressWarnings("unchecked")
	public static Logger getLoggerFromFactory(Class clazz) {
		return loggerFactory.getLogger(clazz);
	}	
	
	public static void setLoggerFactory(ILoggerFactory loggerFactory) {
		SystemProxy.loggerFactory = loggerFactory;
	}
}
