package org.wyki.networking.utility;

public class NetworkAlgorithms {
	/**
	 * Get delay before new attempt at task such as connecting, using well known binary exponential backoff algorithm
	 * @param failedAttempts		Current number of failed attempts
	 * @param firstFailureDelay		Delay after first failure in chosen time unit
	 * @param maxDelay				Maximum allowed delay in chosen time unit
	 * @return
	 */
	public static int getBinaryBackoffDelay(int failedAttempts, int firstFailureDelay, int maxDelay) {
		
		if (failedAttempts <= 0)
			return 0;
		
		if (firstFailureDelay <= 0)
			firstFailureDelay = 500;
		
		if (maxDelay < 0)
			maxDelay = 0;
		
		if (failedAttempts > 16) // watch overflow
			failedAttempts = 16;
		
		int delay = (int) (firstFailureDelay * Math.pow(2, failedAttempts-1));
		
		if (delay > maxDelay)
			delay = maxDelay;
		
		return delay;
	}
	
	/**
	 * Get an expiry time expressed as milliseconds in the future from now. When passing objects containing expiry time fields
	 * between nodes in a cluster, assuming message transmission to be of relatively short and constant time, and some degree of
	 * small inaccuracy to be preferable to the possibility of a large inaccuracy, passing expiry times as offsets into the future
	 * rather than absolute times can be preferable because if one system clock deviates from the norm, passed objects
	 * are still held of the same amount of time. Furthermore, systems like AMF/RTMP compress can compress smaller values, leading
	 * to more efficient network transmission of the values.
	 *
	 * @param expiryTime		The expiry time described as the difference, measured in milliseconds, between that time and midnight, January 1, 1970 UTC
	 * @return					The difference between the current time and the expiry time. <code>0</code> is returned if expiryTime is in the past
	 */
	public static int getExpiryMillisecondsFromNow(long expiryTime) {
		long now = System.currentTimeMillis();
		if (now > expiryTime)
			return 0;
		return (int)(expiryTime - now);
	}
}
