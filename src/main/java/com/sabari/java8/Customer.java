package com.sabari.java8;

public class Customer {
private CustomerId customerId;

public CustomerId getCustomerId() {
    return customerId;
}

public void setCustomerId(CustomerId customerId) {
    this.customerId = customerId;
}

public Customer(CustomerId customerId) {
    super();
    this.customerId = customerId;
}

public void incrementOrders() {
    System.out.println("Now in incrementOrders!!!");
    
}

@Override
public String toString() {
    return "Customer [customerId=" + customerId + "]";
}

}
