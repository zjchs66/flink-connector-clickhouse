package org.apache.flink.connector.clickhouse.entity;

import java.util.HashMap;
import java.util.Map;


public class Operator {

    String db;
    String tablename;
    String opType;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public Map<String, Object> getPr() {
        return pr;
    }

    public void setPr(Map<String, Object> pr) {
        this.pr = pr;
    }

    public Map<String, String> getColumnsType() {
        return columnsType;
    }

    public void setColumnsType(Map<String, String> columnsType) {
        this.columnsType = columnsType;
    }

    public Map<String, Object> getColumnsValue() {
        return columnsValue;
    }

    public void setColumnsValue(Map<String, Object> columnsValue) {
        this.columnsValue = columnsValue;
    }

    public Long getOpts() {
        return opts;
    }

    public void setOpts(Long opts) {
        this.opts = opts;
    }

    Map<String, Object> pr = new HashMap<>();
    Map<String, String> columnsType = new HashMap<>();
    Map<String, Object> columnsValue = new HashMap<>();
    //    Map<String,String> cusColumnsType = new HashMap<>();
//    Map<String,Object> cusColumnsValue = new HashMap<>();
    Long opts;

}
