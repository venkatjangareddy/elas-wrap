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
public class InputException extends EsException {

    public InputException(String ex, int code) {
        super(ex, code);
    }

    public InputException(int code) {
        super(code);
    }

    public InputException(String ex) {
        super(ex);
    }

    public InputException(Throwable ex) {
        super(ex);
    }
}
