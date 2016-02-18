package com.sabari.java8;

public class Book {

    private Author author;
    
    private String title;
    
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Author getAuthor() {
        return author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public Book(Author author) {
        super();
        System.out.println("After Super of Book Construtor");
        this.author = author;
    }

    
    public Book(Author author, String title) {
        super();
        this.author = author;
        this.title = title;
    }

    @Override
    public String toString() {
        return "Book [author=" + author + "]";
    }
    
    
}
