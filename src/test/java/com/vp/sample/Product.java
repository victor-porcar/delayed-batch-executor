package com.vp.sample;

public class Product {

    private Integer id;
    private String description;

    public Integer getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public Product(Integer id, String description) {
        super();
        this.id = id;
        this.description = description;
    }
}
