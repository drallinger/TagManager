package com.drallinger.tagmanager;

import java.util.ArrayList;

public class TagSearch {
    private final ArrayList<Tag> includedTags;
    private final ArrayList<Tag> excludedTags;

    public TagSearch(){
        includedTags = new ArrayList<>();
        excludedTags = new ArrayList<>();
    }

    public void addIncludedTag(Tag tag){
        includedTags.add(tag);
    }

    public void addExcludedTag(Tag tag){
        excludedTags.add(tag);
    }

    public ArrayList<Tag> getIncludedTags() {
        return includedTags;
    }

    public ArrayList<Tag> getExcludedTags() {
        return excludedTags;
    }
}
