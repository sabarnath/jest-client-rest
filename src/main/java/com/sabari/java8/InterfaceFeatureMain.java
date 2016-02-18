package com.sabari.java8;

public class InterfaceFeatureMain {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        Formula formula = new Formula() {
            
            @Override
            public double calculate(int a) {
                // TODO Auto-generated method stub
                return sqrt(a * 100);
            }
        };

        System.out.println("Calculate Result :"+formula.calculate(100));
        System.out.println("Sqrt Result "+formula.sqrt(16));
    }

}
