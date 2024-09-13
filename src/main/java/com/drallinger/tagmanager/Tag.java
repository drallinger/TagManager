package com.drallinger.tagmanager;

public record Tag(int ID, String name) {
    @Override
    public String toString() {
        return name;
    }
}
