package com.example.vectorstoreloader;

public record GameTitle(String title) {

    public String getNormalizedTitle() {
        return title.toLowerCase().replace(" ", "_");
    }
}
