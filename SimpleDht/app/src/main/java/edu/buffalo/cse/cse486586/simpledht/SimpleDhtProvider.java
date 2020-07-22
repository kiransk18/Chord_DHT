package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;
import java.io.BufferedReader;

import android.content.ContentProvider;

import java.net.InetAddress;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.io.OutputStream;

import android.database.Cursor;

import java.io.InputStreamReader;

import android.database.MatrixCursor;
import android.net.Uri;

import java.security.MessageDigest;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.net.ServerSocket;

import android.util.Log;

import java.net.Socket;

import android.os.AsyncTask;

import java.util.Formatter;

import android.telephony.TelephonyManager;

import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;

import android.content.Context;

import java.util.TreeMap;

import android.content.ContentValues;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    HashSet<String> deletedRecords = new HashSet<String>();
    static final String contentProviderURI = "edu.buffalo.cse.cse486586.simpledht.provider";
    static Uri CONTENT_URI;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String runningPort;
    boolean allRecRetrieved = false, singleRecRetrieved = false;
    static HashMap<String, Integer> msgTypeHandler;
    String masterPort = "11108", predNode, succNode;
    DBHelper helper;
    static TreeMap<String, String> currentChordPorts;
    MatrixCursor allRecords;
    boolean isSingle;

    MatrixCursor singleRec;
    HashMap<String, String> portToHashIdMapping;

    @Override
    public boolean onCreate() {

        // Load Message Handler which defines different types of requests made by the nodes.
        msgTypeHandler = new HashMap<String, Integer>();
        loadMsgTypeIdentifier();

        // Load Ports to their HashIds Mapping.
        loadPortToHashIdMapping();

        //fetch the port in which the node is running.
        runningPort = fetchRunningPort();

        // Initially set predecessor and successor to current node.
        predNode = succNode = runningPort;

        //Load DB Helper
        helper = new DBHelper(getContext());

        // Defining the node to be single on creation before joining the chord.
        isSingle = true;

        // A TreeMap which automatically sorts the nodes based on the hashids.
        currentChordPorts = new TreeMap<String, String>();


        //creating server socket
        createServerTask();


        if (!runningPort.equals(masterPort)) {
            StringBuilder sb = new StringBuilder("joinReq");
            sb.append("@" + runningPort);
            sb.append("@" + masterPort);
            Log.i(TAG, "sent join connection");
            invokeClientTask(sb.toString(), masterPort);

        } else {
            currentChordPorts.put(portToHashIdMapping.get(runningPort), runningPort);
        }
        return false;
    }


    public void createServerTask() {
        try {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new ServerSocket(SERVER_PORT));
        } catch (IOException e) {
            Log.e("serverCreateException", "Exception occured while creating server socket");
        }
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("*") || selection.equals("@")) {

            // Deleting all the messages in the node
            helper.deleteAllMessages();

            if (!isSingle && selection.equals("*")) {

                // Passing the deleteAll request to Successor.
                StringBuilder msg = new StringBuilder("deleteAllRecords@");
                msg.append(runningPort);
                invokeClientTask(msg.toString(), succNode);
            }
        }
        else {
            boolean inRange = checkRange(selection);

            if (inRange || isSingle) {
                if (!deletedRecords.contains(selection)) {
                    helper.deleteMessage(selection);
                    deletedRecords.add(selection);
                }
            } else {

                StringBuilder msg = new StringBuilder("deleteMsg@");
                msg.append(selection);
                invokeClientTask(msg.toString(), succNode);
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {

        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.get("key").toString();
        boolean inRange = checkRange(key);
        String value = values.get("value").toString();
        Log.i("Insert key", key);
        if (isSingle || inRange) {
            // If the message lies in the range or the node is the only running process, insert in the current node.
            helper.insertMessage(key, value);
            return uri;
        }

        // If the message lies outside of the range of the node, pass the request to successor.
        StringBuilder msg = new StringBuilder("insert");
        msg.append("@" + key);
        msg.append("@" + value);
        invokeClientTask(msg.toString(), succNode);

        return uri;
    }


    public void loadMsgTypeIdentifier() {

        msgTypeHandler.put("joinReq", 0);
        msgTypeHandler.put("chordUpdate", 1);
        msgTypeHandler.put("insert", 2);
        msgTypeHandler.put("retrieveAllRecords", 3);
        msgTypeHandler.put("retrieveMsg", 4);
        msgTypeHandler.put("deleteAllRecords", 5);
        msgTypeHandler.put("deleteMsg", 6);
        msgTypeHandler.put("retrieveAllResponse", 7);
        msgTypeHandler.put("retrieveMsgResponse", 8);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Cursor cursor;
        if (selection.equals("*") || selection.equals("@")) {

            // Retrieving all the messages stored on this node.
            cursor = helper.retrieveAllMessages();
            if (isSingle || selection.equals("@")) {
                cursor.moveToNext();
                return cursor;
            }

            /* Passing the request to successor if the node is not
             alone or it is request to fetch all dht records. */

            StringBuilder msg = new StringBuilder("retrieveAllRecords");
            msg.append("@" + runningPort);
            allRecords = new MatrixCursor(new String[]{"key", "value"});
            while (cursor.moveToNext()) {
                allRecords.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
            }
            invokeClientTask(msg.toString(), succNode);

            while (!allRecRetrieved) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {

                    Log.i(TAG, "Exception occured while node is waiting for message");
                }
            }

            allRecRetrieved = false;
            return allRecords;
        } else {

            boolean inRange = checkRange(selection);
            if (isSingle || inRange) {
                cursor = helper.retrieveMessage(selection);
                cursor.moveToNext();
                return cursor;
            }
            Log.i("retriveCall ", selection);
            singleRec = new MatrixCursor(new String[]{"key", "value"});
            StringBuilder msg = new StringBuilder("retrieveMsg");
            msg.append("@" + selection);
            msg.append("@" + runningPort);
            invokeClientTask(msg.toString(), succNode);

            while (!singleRecRetrieved) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {

                    Log.i(TAG, "Exception occured while node is waiting for message");
                }
            }

            singleRecRetrieved = false;
            Log.i("retriveFound  ", selection);
            return singleRec;

        }
    }

    // Referred PA1 Code
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            BufferedReader br = null;

            /*
             Referred https://developer.android.com/reference/java/net/ServerSocket
             Referred https://developer.android.com/reference/java/io/BufferedReader
             Referred https://developer.android.com/reference/java/io/PrintWriter
             https://developer.android.com/guide/topics/providers/content-provider-creating
             */
            ContentValues newValues = new ContentValues();
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.scheme("content");
            uriBuilder.authority(contentProviderURI);
            CONTENT_URI = uriBuilder.build();

            try {
                while (true) {
                    Socket soc = serverSocket.accept();
                    //Log.i("accept", "Server accepted connection");
                    br = new BufferedReader(new InputStreamReader(soc.getInputStream()));
                    String msgReceived = br.readLine();
                    String[] msgInfo = msgReceived.split("@");
                    int msgType = msgTypeHandler.get(msgInfo[0]);
                    switch (msgType) {
                        case 0: {

                            //joinReq

                            String hashedEmulatorId = portToHashIdMapping.get(msgInfo[1]);
                            //   Log.i(TAG, hashedEmulatorId);
                            currentChordPorts.put(hashedEmulatorId, msgInfo[1]);
                            List<String> keys = new ArrayList(currentChordPorts.values());
                            int n = keys.size() - 1;
                            // Log.i(TAG,"phase started");
                            int i = keys.indexOf(msgInfo[1]);

                            String predNodeUpdated = (i == 0 ? keys.get(n) : keys.get(i - 1));
                            String succNodeUpdated = (i == n ? keys.get(0) : keys.get(i + 1));
                            String newNode = msgInfo[1];
                            if (predNodeUpdated.equals(masterPort)) {
                                succNode = newNode;
                                isSingle = false;
                            } else {
                                StringBuilder msgForPred = new StringBuilder("chordUpdate");
                                msgForPred.append("@" + "succ");
                                msgForPred.append("@" + newNode);
                                invokeClientTask(msgForPred.toString(), predNodeUpdated);
                            }

                            if (succNodeUpdated.equals(masterPort)) {
                                predNode = newNode;
                                isSingle = false;
                            } else {
                                StringBuilder msgForSucc = new StringBuilder("chordUpdate");
                                msgForSucc.append("@" + "pred");
                                msgForSucc.append("@" + newNode);
                                invokeClientTask(msgForSucc.toString(), succNodeUpdated);
                            }

                            StringBuilder msgForNewNode = new StringBuilder("chordUpdate");
                            msgForNewNode.append("@both");
                            msgForNewNode.append("@" + predNodeUpdated);
                            msgForNewNode.append("@" + succNodeUpdated);
                            invokeClientTask(msgForNewNode.toString(), newNode);


                            break;
                        }
                        case 1: {
                            // Updating Successor or Predecessor or both
                            Log.i(TAG, "chord updated");
                            if (msgInfo[1].equals("pred")) {
                                predNode = msgInfo[2];
                                Log.i(TAG, "predNode updated");
                            } else if (msgInfo[1].equals("succ")) {
                                Log.i(TAG, "succ Node updated");
                                succNode = msgInfo[2];
                            } else {
                                predNode = msgInfo[2];
                                succNode = msgInfo[3];
                                Log.i(TAG, "predNode and succNode updated");
                            }
                            isSingle = false;
                            break;
                        }
                        case 2: {

                            // insert
                            String keyId = msgInfo[1];
                            String data = msgInfo[2];

                            boolean inRange = checkRange(keyId);
                            if (inRange) {
                                // inserting in the current node if the msg lies in the range of this node.
                                newValues.put("key", keyId);
                                newValues.put("value", data);
                                getContext().getContentResolver().insert(CONTENT_URI, newValues);
                                // clientTask("msgInsertedACK@" + runningPort + "@" + keyId, msgInfo[3]);
                                // publishProgress("msgInsertedACK@" + runningPort + "@" + keyId, msgInfo[3]);
                            } else {

                                // Message doesn't lie in the range of the node. Passing it to Successor.
                                invokeClientTask(msgReceived, succNode);

                            }
                            break;
                        }
                        case 3: {
                            String receiverPort = msgInfo[1];
                            if (runningPort.equals(receiverPort)) // Request has been passed between all the nodes.
                                allRecRetrieved = true;
                            else {
                                // Cursor temp = getContext().getContentResolver().query(CONTENT_URI, null, "*", null, null);
                                Cursor resp = helper.retrieveAllMessages();

                                StringBuilder sb = new StringBuilder("retrieveAllResponse");
                                while (resp.moveToNext()) {
                                    sb.append("@" + resp.getString(0).trim() + "#" + resp.getString(1).trim());
                                }

                                // Send the current node data to the requested node.
                                invokeClientTask(sb.toString(), receiverPort);

                                //send the Message request to succesor node.
                                invokeClientTask(msgReceived, succNode);
                            }
                            break;
                        }

                        case 4: {
                            // retrieveMsg
                            String msgId = msgInfo[1];
                            String senderPort = msgInfo[2];
                            boolean inRange = checkRange(msgId);
                            if (inRange) {
                                Cursor temp = getContext().getContentResolver().query(CONTENT_URI, null, msgId, null, null);
                                if (temp != null) {
                                    Log.i(TAG, "Msg Found in this node and sent back to sender");
                                    String key = temp.getString(0);
                                    String value = temp.getString(1);
                                    StringBuilder msgToSend = new StringBuilder("retrieveMsgResponse" + "@" + key + "@" + value);
                                    invokeClientTask(msgToSend.toString(), senderPort);
                                    temp.close();
                                }
                            } else {
                                invokeClientTask(msgReceived, succNode);
                            }
                            break;
                        }
                        case 5: {
                            // deleteAllRecords
                            Log.i(TAG, "delete all records executed");
                            if (!runningPort.equals(msgInfo[1])) {
                                helper.deleteAllMessages();
                                invokeClientTask(msgReceived, succNode);
                            }

                            break;
                        }

                        case 6: {
                            //delete single record
                            String msg = msgInfo[1];
                            boolean inRange = checkRange(msg);

                            if (inRange) {
                                if (!deletedRecords.contains(msg)) {
                                    Log.i(TAG, "deleting record");
                                    helper.deleteMessage(msg);
                                    deletedRecords.add(msg);
                                }
                            } else {
                                Log.i(TAG, "passing delete req to successor");

                                invokeClientTask(msgReceived, succNode);
                            }
                            break;

                        }

                        case 7: {

                            String[] result = msgReceived.split("@");
                            Log.v(TAG, "Inside retrieveAll Response");
                            Log.i("recvAllResp", msgReceived);
                            for (int i = 1; i < result.length; i++) {
                                String temp = result[i];
                                Log.i(TAG, temp);
                                String[] keyValue = temp.split("#");
                                String key = keyValue[0];
                                String value = keyValue[1];
                                allRecords.newRow()
                                        .add("key", key)
                                        .add("value", value);
                            }
                            break;
                        }

                        case 8: {
                            Log.i("msgRetrieved", "msg found");
                            singleRec.newRow()
                                    .add("key", msgInfo[1])
                                    .add("value", msgInfo[2]);
                            singleRecRetrieved = true;
                            break;
                        }

                    }
                    OutputStream op = soc.getOutputStream();
                    PrintWriter pw = new PrintWriter(op, true);
                    pw.println("ACK");

                }
            } catch (Exception ex) {
                Log.e("SERVER-NA", ex.getMessage());
            }

            return null;
        }

        //Referred PA1 code
        protected void onProgressUpdate(String... strings) {
            String msg = strings[0];
            String node = strings[1];
            new ClientTaskOnCreate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node);
            return;
        }

    }

    // Referred PA1 code
    public String fetchRunningPort() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String lineNumber = tel.getLine1Number();
        String portId = lineNumber.substring(tel.getLine1Number().length() - 4);
        return "" + Integer.valueOf(portId) * 2;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        return 0;
    }

    // Given
    private String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException ex) {
            Log.i("genHash", "NoSuchAlgorithm Exception occured while hashing");
        }
        return null;
    }

    private void loadPortToHashIdMapping() {
        portToHashIdMapping = new HashMap<String, String>();
        portToHashIdMapping.put("11108", genHash("5554"));
        portToHashIdMapping.put("11112", genHash("5556"));
        portToHashIdMapping.put("11116", genHash("5558"));
        portToHashIdMapping.put("11120", genHash("5560"));
        portToHashIdMapping.put("11124", genHash("5562"));
    }


    // Referred PA1 Code
    private class ClientTaskOnCreate extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String remotePort = msgs[1];
                final Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                String msgToSend = msgs[0];

                // Referred https://developer.android.com/reference/java/net/Socket
                // Referred https://developer.android.com/reference/java/io/BufferedReader
                // Referred https://developer.android.com/reference/java/io/PrintWriter

                OutputStream op = socket.getOutputStream();
                PrintWriter pw = new PrintWriter(op, true);
                pw.println(msgToSend);
                // Log.i("clientmsg","Client sent message to the server");
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String response = br.readLine();
                if (response != null && response.equals("ACK")) {
                    socket.close();
                    //  Log.i("disconnect","Closing client socket");
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            return null;
        }
    }

    public boolean checkRange(String key) {

        // Check if the message lies in the range of the current node.
        String predHash = portToHashIdMapping.get(predNode);
        String currNodeHash = portToHashIdMapping.get(runningPort);
        String hashedKey = genHash(key);
        if (hashedKey.compareTo(predHash) > 0 && hashedKey.compareTo(currNodeHash) <= 0)
            return true;
        else if (predHash.compareTo(currNodeHash) > 0 && (hashedKey.compareTo(predHash) > 0 || hashedKey.compareTo(currNodeHash) <= 0))
            return true;

        return false;
    }

    public void invokeClientTask(String msg, String port) {
        new ClientTaskOnCreate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
    }
}
