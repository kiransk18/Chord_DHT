package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;
import android.database.DatabaseUtils;

public class DBHelper  extends SQLiteOpenHelper {

    public static final String DB_NAME = "nodedata.db";
    public static final String TABLE_NAME = "messages_table";
    public static final String KEY_ID = "key";
    public static final String MESSAGE = "value";
    static final String TAG = DBHelper.class.getSimpleName();
    public DBHelper(Context context) {
        super(context, DB_NAME, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + TABLE_NAME + "(ID INTEGER PRIMARY KEY AUTOINCREMENT," + KEY_ID + " TEXT NOT NULL UNIQUE, value TEXT NOT NULL)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    public  boolean insertMessage(String key, String value)
    {
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues cv = new ContentValues();
        cv.put(KEY_ID, key);
        cv.put(MESSAGE, value);
        long response = db.insertWithOnConflict(TABLE_NAME, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
        //  db.close();
        if(response == - 1) {
            Log.i(TAG, "insertion Failed");
            return false;
        }
        //    Log.i(TAG, "insertion Success@" + key);
        return true;
    }

    public  Cursor retrieveMessage(String key)
    {
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor cursor = db.rawQuery("SELECT key, value  FROM messages_table WHERE TRIM(key) = '"+key.trim()+"'", null);
        //Log.v("Cursor Object", DatabaseUtils.dumpCursorToString(cursor));
        // db.close();
        return cursor;
    }

    public  Cursor retrieveAllMessages()
    {
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor cursor = db.rawQuery("SELECT key, value  FROM messages_table", null);
        //Log.v("Cursor Object", DatabaseUtils.dumpCursorToString(cursor));
        // db.close();
        return cursor;
    }

    public  int deleteMessage(String key)
    {
        SQLiteDatabase db = this.getReadableDatabase();
        int res = 0;
        //  db.rawQuery("DELETE FROM messages_table WHERE TRIM(key) = '"+key.trim()+"'", null);
        res = db.delete("messages_table", "key=?", new String[]{key});
        //Log.v("Cursor Object", DatabaseUtils.dumpCursorToString(cursor));
        // db.close();
        return res;
    }

    public  int deleteAllMessages()
    {
        SQLiteDatabase db = this.getReadableDatabase();
        int res = 0;
        res = db.delete("messages_table", null, null);
        //db.rawQuery("DELETE FROM messages_table", null);
        //Log.v("Cursor Object", DatabaseUtils.dumpCursorToString(cursor));
        // db.close();
        return res;
    }

}

