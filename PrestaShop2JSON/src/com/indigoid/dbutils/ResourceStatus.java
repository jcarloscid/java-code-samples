package com.indigoid.dbutils;

/**
 * List of possible statuses for a resource.
 * 
 * @author Charlie
 *
 */
public enum ResourceStatus {
	/**
	 * The resource has been allocated but has never being used.
	 */
	UNUSED,
	/**
	 * The resource has been set for a particular purpose and can only be reused for that purpose again.
	 */
	PRESET,
	/**
	 * The resource is currently in used and its content is valid.
	 */
	BUSY,
	/**
	 * The resource has been used but its content is no longer valid, so it can be
	 * reused by a new consumer.
	 */
	RECYCLABLE;
}