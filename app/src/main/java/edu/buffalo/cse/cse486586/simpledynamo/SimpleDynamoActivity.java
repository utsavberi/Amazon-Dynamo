package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.security.NoSuchAlgorithmException;

public class SimpleDynamoActivity extends Activity {
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dynamo);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        TextView tv2 = (TextView) findViewById(R.id.response_tv);
        tv2.setMovementMethod(new ScrollingMovementMethod());

        final EditText send_txt = (EditText) findViewById(R.id.send_txt);
        final TextView response_tv = (TextView) findViewById(R.id.response_tv);

        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                ContentResolver mContentResolver = getContentResolver();
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                Cursor resultCursor = mContentResolver.query(mUri, null, "\"@\"", null, null);
                response_tv.setText("");
                resultCursor.moveToFirst();
                while (!resultCursor.isAfterLast()) {
                    Log.d(TAG, resultCursor.getString(0) + " : " + resultCursor.getString(1));
                    response_tv.append(resultCursor.getString(0) + " : " + resultCursor.getString(1) + "\n");
                    resultCursor.moveToNext();
                }
            }

        });

        findViewById(R.id.send_btn).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String txt = send_txt.getText().toString();
                ContentValues cv = new ContentValues();
                cv.put("key", txt.split(",")[0]);
                cv.put("value", txt.split(",")[0]);
                try {
                    response_tv.append("inserted " + MyUtils.genHash(txt));
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                ContentResolver mContentResolver = getContentResolver();
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                mContentResolver.insert(mUri, cv);
            }
        });

        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                ContentResolver mContentResolver = getContentResolver();
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        "\"*\"", null, null);
                Log.d(TAG, "Gdump");
                resultCursor.moveToFirst();
                response_tv.setText("");
                while (!resultCursor.isAfterLast()) {
                    Log.d(TAG, resultCursor.getString(0) + " : " + resultCursor.getString(1));
                    response_tv.append(resultCursor.getString(0) + " : " + resultCursor.getString(1) + "\n");
                    resultCursor.moveToNext();
                }
            }

        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

    public void onStop() {
        super.onStop();
        Log.v("Test", "onStop()");
    }

}
