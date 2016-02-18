/**
 * 
 */
package com.tryout.jest;

/**
 * @author smi-user
 *
 */
public class BookNotFoundException extends Exception{

    private String message;
    
    
    public String getMessage() {
        return message;
    }


    public void setMessage(String message) {
        this.message = message;
    }


    /**
     * 
     */
    private static final long serialVersionUID = 1L;


    public BookNotFoundException(String message) {
        super();
        this.message = message;
    }
    
    

    
}
