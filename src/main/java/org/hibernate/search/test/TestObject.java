package org.hibernate.search.test;

public class TestObject {

    private String query;
    private int authorFacetCount;
    private int authorFacetCount2;
    private int publisherFacetCount;
    private int publisherFacetCount2;

    public TestObject(String string, int i, int j, int k , int l) {
        this.query = string;
        this.authorFacetCount = i;
        this.authorFacetCount2 = j;
        this.publisherFacetCount = k;
        this.publisherFacetCount2 = l;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getAuthorFacetCount() {
        return authorFacetCount;
    }

    public void setAuthorFacetCount(int authorFacetCount) {
        this.authorFacetCount = authorFacetCount;
    }

    public int getAuthorFacetCount2() {
        return authorFacetCount2;
    }

    public void setAuthorFacetCount2(int authorFacetCount2) {
        this.authorFacetCount2 = authorFacetCount2;
    }

    public int getPublisherFacetCount() {
        return publisherFacetCount;
    }

    public void setPublisherFacetCount(int publisherFacetCount) {
        this.publisherFacetCount = publisherFacetCount;
    }

    public int getPublisherFacetCount2() {
        return publisherFacetCount2;
    }

    public void setPublisherFacetCount2(int publisherFacetCount2) {
        this.publisherFacetCount2 = publisherFacetCount2;
    }
    
    

}
