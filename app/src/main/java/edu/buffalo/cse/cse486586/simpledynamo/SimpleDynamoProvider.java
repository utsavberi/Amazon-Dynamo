package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;

import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse.cse486586.simpledynamo.Repositories.ContextRepository;
import edu.buffalo.cse.cse486586.simpledynamo.core.Dynamo;

/**
 * Provides access of the Dynamo to the UI Activity
 */
public class SimpleDynamoProvider extends ContentProvider {

    Dynamo dynamo;

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        dynamo = new Dynamo(10000, Integer.parseInt(portStr) * 2, new ContextRepository(getContext()));

        dynamo.addNode("11108", "11112", "11116");
        dynamo.addNode("11120", "11116", "11124");
        dynamo.addNode("11112", "11124", "11108");
        dynamo.addNode("11116", "11108", "11120");
        dynamo.addNode("11124", "11120", "11112");

        dynamo.start();
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        switch (selection) {
            case "\"@\"":
                dynamo.removeAllLocal();
                break;
            case "\"*\"":
                dynamo.removeAllGlobal();
                break;
            default:
                dynamo.remove(selection);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        dynamo.put(key, value);
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        switch (selection) {
            case "\"@\"":
                return hashMapToCursor(dynamo.getAllLocal());
            case "\"*\"":
                return hashMapToCursor(dynamo.getAllGlobal());
            default:
                MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                cursor.addRow(new Object[]{selection, dynamo.getValue(selection)});
                return cursor;
        }
    }

    private Cursor hashMapToCursor(HashMap<String, String> map) {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        for (Map.Entry<String, String> e : map.entrySet()) {
            cursor.addRow(new Object[]{e.getKey(), e.getValue()});
        }
        return cursor;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }


}
