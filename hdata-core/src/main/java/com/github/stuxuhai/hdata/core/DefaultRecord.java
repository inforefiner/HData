package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.Record;

public class DefaultRecord implements Record {

    private final Object[] values;
    private int cursor;

    public DefaultRecord(int fieldCount) {
        values = new Object[fieldCount];
    }

    @Override
    public void add(int index, Object field) {
        values[index] = field;
        this.cursor++;
    }

    @Override
    public Object[] values() {
        return values;
    }

    @Override
    public String[] strings() {
        String[] arr = new String[values.length];
        for(int i = 0; i < values.length; i ++){
            if(values[i] != null)
                arr[i] = values[i].toString();
            else{
                arr[i] = "";
            }
        }
        return arr;
    }

    @Override
    public void add(Object field) {
        add(cursor, field);
    }

    @Override
    public Object get(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return values.length;
    }
}
