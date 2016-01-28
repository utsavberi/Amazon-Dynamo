package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by utsav on 4/20/15.
 */
public class MyUtils {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    public static String getNodeIdFromPort(String port) {
        return Integer.toString(Integer.parseInt(port) / 2);
    }

    public static String readFile(InputStream filename) {
        String tmp = "";
        BufferedReader rdr = null;
        try {
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

    public static void writeToFile(FileOutputStream key, String value) {
        FileOutputStream outputStream;
        try {
            outputStream = key;
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (IOException e) {
            Log.e(TAG, "File write failed" + e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    public static HashMap<String, String> convertToHashMap(String s) {
        HashMap<String, String> result = new HashMap<>();
        if (s == null || s.trim().isEmpty()) return result;
        String[] rows = s.split("\\|");
        for (String row : rows) {
            if (row.trim().isEmpty()) continue;
            String[] keyValue = row.split(",");
            result.put(keyValue[0], keyValue[1]);
        }
        return result;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static String hashMapToString(HashMap<String, String> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            e.getKey();
            sb.append(",");
            e.getValue();
            sb.append("|");
        }
        return sb.toString();
    }
}
