/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.exceptions;

/**
 *
 * @author SKashyap
 *
 */
public class UnAuthorizedAccessException extends EsException {

    public UnAuthorizedAccessException(String ex, int code) {
        super(ex, code);
    }

    public UnAuthorizedAccessException(int code) {
        super(code);
    }

    public UnAuthorizedAccessException(String ex) {
        super(ex);
    }

    public UnAuthorizedAccessException(Throwable ex) {
        super(ex);
    }
}
