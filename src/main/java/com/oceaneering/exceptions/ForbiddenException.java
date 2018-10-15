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
public class ForbiddenException extends EsException {

    public ForbiddenException(String ex, int code) {
        super(ex, code);
    }

    public ForbiddenException(int code) {
        super(code);
    }

    public ForbiddenException(String ex) {
        super(ex);
    }

    public ForbiddenException(Throwable ex) {
        super(ex);
    }
}
