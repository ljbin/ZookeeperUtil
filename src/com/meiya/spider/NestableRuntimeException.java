/**   
* Copyright (c) 2012-2022 厦门市美亚柏科信息股份有限公司.
*/
package com.meiya.spider;

/**
 * @Description: 其他运行时异常
 * @Creator：linjb 2016-5-30
 */
public class NestableRuntimeException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1816610634814876133L;

    public NestableRuntimeException() {
        super();
        // TODO Auto-generated constructor stub
    }

    public NestableRuntimeException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        // TODO Auto-generated constructor stub
    }

    public NestableRuntimeException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    public NestableRuntimeException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public NestableRuntimeException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

      
}
