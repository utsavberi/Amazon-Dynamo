package edu.buffalo.cse.cse486586.simpledynamo.Repositories;

import android.content.Context;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by utsav on 4/1/15.
 */
public class ContextRepository implements KeyValueRepository {
    Context ctx;

    public ContextRepository(Context ctx) {
        this.ctx = ctx;
    }

    @Override
    public void store(String key, String value) {
        FileOutputStream outputStream;
        try {
            outputStream = ctx.openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String fetch(String key) {
        InputStream filename;
        String tmp = "";
        BufferedReader rdr = null;
        try {
            filename = ctx.openFileInput(key);
            rdr = new BufferedReader(new InputStreamReader(filename));
            String line;
            while ((line = rdr.readLine()) != null) {
                tmp += line + "\n";
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (rdr != null) {
                try {
                    rdr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tmp;
    }

    @Override
    public HashMap<String, String> fetchAll() {
        HashMap<String, String> result = new HashMap<>();
        for (File file : ctx.getFilesDir().listFiles()) {
            result.put(file.getName(), fetch(file.getName()));
        }
        return result;
    }

    @Override
    public void remove(String key) {
        ctx.deleteFile(key);
    }

    @Override
    public void removeAll() {
        for (File file : ctx.getFilesDir().listFiles()) {
            ctx.deleteFile(file.getName());
        }
    }
}
