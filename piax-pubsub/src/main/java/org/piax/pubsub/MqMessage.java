/*
 * MqMessage.java - A message implementation.
 * 
 * Copyright (c) 2016 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piax.pubsub;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class MqMessage implements Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = -4783439070342174271L;
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private boolean mutable = true;
    private byte[] payload;
    private int qos = 1;
    private boolean retained = false;
    private boolean dup = false;

    public static void validateQos(int qos) {
        if ((qos < 0) || (qos > 2)) {
            throw new IllegalArgumentException();
        }
    }

    public MqMessage(String topic) {
        setTopic(topic);
        setPayload(new byte[] {});
    }

    public MqMessage(String topic, byte[] payload) {
        setTopic(topic);
        setPayload(payload);
    }

    public byte[] getPayload() {
        return payload;
    }

    public void clearPayload() {
        checkMutable();
        this.payload = new byte[] {};
    }

    public void setPayload(byte[] payload) {
        checkMutable();
        if (payload == null) {
            throw new NullPointerException();
        }
        this.payload = payload;
    }

    public boolean isRetained() {
        return retained;
    }

    public void setRetained(boolean retained) {
        checkMutable();
        this.retained = retained;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        checkMutable();
        validateQos(qos);
        this.qos = qos;
    }

    public String toString() {
        return new String(payload);
    }

    protected void setMutable(boolean mutable) {
        this.mutable = mutable;
    }

    protected void checkMutable() throws IllegalStateException {
        if (!mutable) {
            throw new IllegalStateException();
        }
    }

    protected void setDuplicate(boolean dup) {
        this.dup = dup;
    }

    public boolean isDuplicate() {
        return this.dup;
    }

    public void recordTimestamp() {
        OffsetDateTime odt = OffsetDateTime.now();
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();
        StackTraceElement callingElement = stes[2];
        if (callingElement.getClassName().equals("NestedMessage")){
            callingElement = stes[3];
        }

        String timestamp = odt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "@" + callingElement + "/";
        byte[] timestampBytes = timestamp.getBytes();

        int insert = 0;
        byte[] origin = getPayload();
        byte[] modified = Arrays.copyOf(origin, origin.length);

        for (int i = 0; i < origin.length; i++) {
            if (origin[i] == 47 && i + 1 < origin.length) {
                if (origin[i + 1] < 48 || origin[i + 1] > 57) {
                    insert = i + 1;
                    break;
                }
            }
        }
        if (insert + timestamp.getBytes().length > origin.length) {
            // TODO: should throw exceptions
        }
        System.arraycopy(timestampBytes, 0, modified, insert, timestampBytes.length);
        setPayload(modified);
    }
}
