/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class BigDawgException extends Exception {
	/**
	 * generated serial version id
	 */
	private static final long serialVersionUID = -8226144079631352364L;

	public BigDawgException(String msg) {
		super(msg);
	}
	
	public BigDawgException(String message, Throwable cause) {
        super(message, cause);
    }
}
