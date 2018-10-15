/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.exceptions;

import com.oceaneering.errors.ErrorCodes;

/**
 *
 * @author SKashyap
 *
 */
public class EsException extends Exception {

    protected int code = 0;

    public EsException() {
        super();
    }

    public EsException(String string, int code) {
        super(new String(string).concat(ErrorCodes.getMEssageFromCode(code)));
        this.code = code;
    }

    public EsException(int code) {

        super(ErrorCodes.getMEssageFromCode(code));
        this.code = code;
    }

    public EsException(String string) {
        super(string);
    }

    public EsException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public EsException(Throwable thrwbl) {
        super(thrwbl);
    }

    protected EsException(String string, Throwable thrwbl, boolean bln, boolean bln1) {
        super(string, thrwbl, bln, bln1);
    }

    public int getCode() {
        return this.code;

    }
}
