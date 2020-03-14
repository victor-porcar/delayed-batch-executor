package com.github.victormpcmun.delayedbatchexecutor.sample;

public class Product {

    private final Integer id;
    private final String description;

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
