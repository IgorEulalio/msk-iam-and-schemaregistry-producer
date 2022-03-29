package com.igoreul.producer;


import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class UserDTO {

    @JsonProperty("name")
    @NotNull
    private String name;

    @NotNull
    @JsonProperty("favorite_color")
    private String favoriteColor;

    @NotNull
    @JsonProperty("favorite_number")
    private int favoriteNumber;

    public UserDTO(String name, String favoriteColor, int favoriteNumber) {
        this.name = name;
        this.favoriteColor = favoriteColor;
        this.favoriteNumber = favoriteNumber;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }
}
