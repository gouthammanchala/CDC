/**
 * 
 */
package com.rpamballa.cdc.exceptions;

/**
 * @author revanthpamballa
 * 
 */
public class CDCException extends Exception {

	public CDCException(Exception e) {
		super(e);
		return;
	}

	public CDCException(String msg) {
		super(msg);
		return;
	}

	public CDCException(String msg, Exception e) {
		super(msg, e);
		return;
	}

}
