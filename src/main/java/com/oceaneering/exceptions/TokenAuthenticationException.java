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
public class TokenAuthenticationException extends EsException {

    public TokenAuthenticationException(String ex, int code) {
        super(ex, code);
    }

    public TokenAuthenticationException(int code) {
        super(code);
    }

    public TokenAuthenticationException(String ex) {
        super(ex);
    }

    public TokenAuthenticationException(Throwable ex) {
        super(ex);
    }
}
