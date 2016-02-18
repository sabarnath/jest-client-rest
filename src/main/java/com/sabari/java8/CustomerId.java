package com.sabari.java8;

public class CustomerId {

    private String id;

    public CustomerId(String id) {
        // TODO Auto-generated constructor stub
        System.out.println("Now in CustomerId construt,"+id);
        this.setId(id);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "CustomerId [id=" + id + "]";
    }

    
    
    
}
