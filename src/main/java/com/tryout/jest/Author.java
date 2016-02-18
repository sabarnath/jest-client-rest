package com.tryout.jest;

public class Author {
private String surname;

public Author(String surname) {
    // TODO Auto-generated constructor stub
    System.out.println("Now in Author Construtor");
    this.setSurname(surname);
}

public String getSurname() {
    return surname;
}

public void setSurname(String surname) {
    this.surname = surname;
}

@Override
public String toString() {
    return "Author [surname=" + surname + "]";
}


}
