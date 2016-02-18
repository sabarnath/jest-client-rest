package com.sabari.java8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class JavaEightTest {

    private final Map<CustomerId, Customer> customers = new HashMap<CustomerId, Customer>();
    public void incrementCustomerOrders(CustomerId customerId) {
        System.out.println("Now in incrementCustomerOrders : "+customerId.getId());
        Customer customer = customers.get(customerId);
        if (customer == null) {
            customer = new Customer(customerId);
            customers.put(customerId, customer);
        }
        customer.incrementOrders();
        System.out.println("customers :"+customers.toString());
    }
    
    public static void main(String... args) {
        // TODO Auto-generated method stub
        JavaEightTest jav = new JavaEightTest();
        /*CustomerId cus = new CustomerId("123");
        jav.incrementCustomerOrders(cus);
        CustomerId cus1 = new CustomerId("123");
        jav.incrementCustomerOrdersWithLamda(cus1);
        CustomerId cus2 = new CustomerId("123");
        jav.incrementCustomerOrdersWithMethodRef(cus2);
        jav.listTesting();
        jav.getAllAuthorsAlphabetically();
        jav.getAllAuthorsAlphabeticallyStrem();
        jav.getAllAuthorsAlphabeticallyMethodRef();*/
        jav.findBook();
    }

    public void incrementCustomerOrdersWithLamda(CustomerId customerId) {
        System.out.println("Now in incrementCustomerOrdersWithLamda : "+customerId.getId());
        Customer customer = customers.computeIfAbsent(customerId,
               id -> new Customer(id));
        customer.incrementOrders();
        System.out.println("customers :"+customers.toString());

    }
    public void incrementCustomerOrdersWithMethodRef(CustomerId customerId) {
        System.out.println("Now in incrementCustomerOrdersWithMethodRef : "+customerId.getId());
        Customer customer = customers.computeIfAbsent(customerId, Customer::new);
        customer.incrementOrders();
        System.out.println("customers :"+customers.toString());

    }
    public void listTesting(){
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hello", new CustomerId("123"));
        map.computeIfAbsent("me", key -> map.put(key, new CustomerId("321")));
        System.out.println("Map computeIfAbsent:"+map.toString());
        map.computeIfAbsent("me", CustomerId::new);
        System.out.println("Map computeIfAbsent:"+map.toString());
        
    }
    public List<Author> getAllAuthorsAlphabetically() {
        List<Book> books = prepareBooks();
        List<Author> authors = new ArrayList<Author>();
        for (Book book : books) {
            Author author = book.getAuthor();
            if (!authors.contains(author)) {
                authors.add(author);
            }
        }
        Collections.sort(authors, new Comparator<Author>() {
            public int compare(Author o1, Author o2) {
                return o1.getSurname().compareTo(o2.getSurname());
            }
        });
        System.out.println("authors : "+authors.toString());
        return authors;
    }

    private List<Book> prepareBooks() {
        List<Book> books = new ArrayList<Book>();
        books.add(new Book(new Author("sounder")));
        books.add(new Book(new Author("Arujun")));
        books.add(new Book(new Author("mani")));
        books.add(new Book(new Author("sabari")));
        books.add(new Book(new Author("nathan")));
        books.add(new Book(new Author("zahir")));
        books.add(new Book(new Author("bala")));
        books.add(new Book(new Author("bala")));
        return books;
    }
    private List<Book> prepareBooksWithAllFieldConstrutor() {
        List<Book> books = new ArrayList<Book>();
        books.add(new Book(new Author("sounder"),"Head first java"));
        books.add(new Book(new Author("Arujun"),"Action2"));
        books.add(new Book(new Author("mani"),"Spring4"));
        books.add(new Book(new Author("sabari"),"Struts2"));
        books.add(new Book(new Author("nathan"),"Storm"));
        books.add(new Book(new Author("zahir"),"Big Data"));
        books.add(new Book(new Author("bala"),"Manual Testing"));
        return books;
    }
    public void getAllAuthorsAlphabeticallyStrem() {
        List<Book> books = prepareBooks();
         
        List<Author> authors = books.stream()
                    .map(book -> book.getAuthor())
                    .distinct()
                    .sorted((o1, o2) -> o1.getSurname().compareTo(o2.getSurname()))
                    .collect(Collectors.toList());
        System.out.println("Authors :"+authors.toString());
    }
    public void getAllAuthorsAlphabeticallyMethodRef() {
        List<Book> books = prepareBooks();
        List<Author> authors = books.stream()
                    .map(Book::getAuthor)
                    .distinct()
                    .sorted(Comparator.comparing(Author::getSurname))
                    .collect(Collectors.toList());
        System.out.println("Authors :"+authors.toString());
    }
    public void findBook() {
        List<Book> books = prepareBooksWithAllFieldConstrutor();
        try {
            System.out.println("Result :"+findBookByTitle(books, "Head first java1"));
        } catch (BookNotFoundException e) {
            System.out.println("Error result :"+e.getMessage());
        }
    }
    public Book findBookByTitle(List<Book> books, String title) throws BookNotFoundException {
        Optional<Book> foundBook = books.stream()
               .filter(book -> book.getTitle().equals(title))
               .findFirst();
        return foundBook.orElseThrow(() -> new BookNotFoundException("Did not find book with title " + title));
    }
}
