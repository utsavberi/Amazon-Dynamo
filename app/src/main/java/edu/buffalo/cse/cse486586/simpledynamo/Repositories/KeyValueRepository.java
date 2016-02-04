package edu.buffalo.cse.cse486586.simpledynamo.Repositories;

import java.util.HashMap;

/**
 * Created by utsav on 4/1/15.
 */
public interface KeyValueRepository {
    public void store(String key, String value);

    public String fetch(String key);

    public HashMap<String, String> fetchAll();

    public void remove(String key);

    public void removeAll();
}
