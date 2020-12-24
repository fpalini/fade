package fade.util;

import org.apache.spark.SparkConf;

import java.util.NoSuchElementException;

public class Configuration extends SparkConf {
    public void setString(String key, String value) {
        set(key, value);
    }
    public void setInt(String key, int value) {
        set(key, value+"");
    }
    public void setBoolean(String key, boolean value) {
        set(key, value+"");
    }
    public void setDouble(String key, double value) {
        set(key, value+"");
    }
    public void setLong(String key, long value) {
        set(key, value+"");
    }


    public String getString(String key) {
        try { return get(key); }
        catch(NoSuchElementException e) { return null; }
    }

    public Integer getInt(String key) {
        try { return Integer.parseInt(get(key)); }
        catch(NoSuchElementException e) { return null; }
    }

    public Boolean getBoolean(String key) {
        try { return Boolean.parseBoolean(get(key)); }
        catch(NoSuchElementException e) { return null; }
    }

    public Double getDouble(String key) {
        try { return Double.parseDouble(get(key)); }
        catch(NoSuchElementException e) { return null; }
    }

    public String getString(String key, String value) {
        try { return get(key); }
        catch(NoSuchElementException e) { return value; }
    }


    public Long getLong(String key) {
        try { return Long.parseLong(get(key)); }
        catch(NoSuchElementException e) { return null; }
    }

    public int getInt(String key, int value) {
        try { return Integer.parseInt(get(key)); }
        catch(NoSuchElementException e) { return value; }
    }

    public boolean getBoolean(String key, boolean value) {
        try { return Boolean.parseBoolean(get(key)); }
        catch(NoSuchElementException e) { return value; }
    }

    public double getDouble(String key, double value) {
        try { return Double.parseDouble(get(key)); }
        catch(NoSuchElementException e) { return value; }
    }

    public long getLong(String key, long value) {
        try { return Long.parseLong(get(key)); }
        catch(NoSuchElementException e) { return value; }
    }
}
