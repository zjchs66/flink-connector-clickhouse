package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.entity.Operator;
import org.apache.flink.connector.clickhouse.util.JsonUtils;
import org.apache.flink.connector.clickhouse.util.TemporalConversions;
import org.apache.flink.table.data.RowData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author zhoujuncheng
 * @date 2023/5/4
 */
public class CommonOperator {
    public static Object covertValue(Operator op, Map.Entry<String, Object> entry) {
        Object o = entry.getValue();
        String type = op.getColumnsType().get(entry.getKey().toLowerCase());
        if (type == null) {
            return o;
        }
        switch (type) {
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Time":
                op.getColumnsType().put(entry.getKey().toLowerCase(), "String");
                if (o == null) {
                    return o;
                }
                Long l = 0L;
                if (o instanceof Integer) {
                    l = ((Integer) o).longValue();
                }
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
                simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));
                String t = simpleDateFormat.format(l);
                return t;
            case "Date":
                if (o == null) {
                    return o;
                }
                if (o instanceof String) {
                    return Date.valueOf((String) o);
                }

                LocalDate localDate = TemporalConversions.toLocalDate(o);
                return localDate.format(DateTimeFormatter.ISO_DATE);
            case "TIMESTAMP":
            case "DateTime64":
                if (o == null) {
                    return o;
                }
                Long timestamp = Long.valueOf(String.valueOf(o));
                SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                simpleDateFormat2.setTimeZone(TimeZone.getTimeZone("GMT+0"));
                String t2 = simpleDateFormat2.format(timestamp);
                return t2;
            default:
                if (type.contains("Decimal")) {
                    try {
                        if (o == null) {
                            return o;
                        }

                        int scale = 0;
                        String encoded = "";
                        if (o instanceof Map) {
                            scale = (Integer) ((Map) o).get("scale");
                            encoded = (String) ((Map) o).get("value");
                        } else {
                            encoded = (String) o;
                        }
                        final BigDecimal decoded = new BigDecimal(new BigInteger(Base64.getDecoder().decode(encoded)), scale);
                        return decoded;
                    } catch (Exception e) {
                        System.out.println(e);
                    }

                }
                return o;

        }
    }

    public static String columnTypeMapper(String type, Map<String, Object> param) throws Exception {
        switch (type) {
            case "boolean":
                return "UInt8";
            case "int8":
                return "Int8";
            case "int16":
                return "Int16";
            case "int32":
                return "Int32";
            case "int64":
                return "Int64";
            case "float32":
                return "float";
            case "float64":
            case "double":
                return "double";
            case "io.debezium.data.Json":
                return "json";
            case "io.debezium.data.VariableScaleDecimal":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal":
                return "Decimal64(5)";
            case "string":
            case "io.debezium.data.Xml":
            case "io.debezium.time.ZonedTimestamp":
            case "bytes":
                return "String";
            case "io.debezium.time.Date":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Date":
                return "Date";
            case "io.debezium.time.Time":
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.NanoTime":
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
            case "io.debezium.time.NanoTimestamp":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Timestamp":
                return "DateTime64";
            case "io.debezium.time.ZonedTime":
            default:
                return type;
        }
    }

    public static Operator deserialize(RowData rowData) throws Exception {
        return deserialize(rowData.getString(0).toBytes());
    }

    public static Operator deserialize(byte[] bytes) throws Exception {

        Operator sqlOperator = new Operator();
        if (bytes == null) {
            return null;
        }
        Map<String, Object> msg = JsonUtils.json2Map(bytes);

        if (msg != null) {
            for (String v : msg.keySet()) {
                if (v.equals("schema")) {
                    Map<String, Object> m = (Map<String, Object>) msg.get(v);
                    for (String v1 : m.keySet()) {
                        if (v1.equals("fields")) {
                            List<Map<String, Object>> m1 = (List<Map<String, Object>>) m.get(v1);
                            for (Map<String, Object> v2 : m1) {

                                List<Map<String, Object>> f = (List<Map<String, Object>>) v2.get("fields");
                                String key = (String) v2.get("field");
                                if (key.equals("after")) {
                                    for (Map<String, Object> f1 : f) {
                                        String fieldType = f1.containsKey("name") ? (String) f1.get("name") : (String) f1.get("type");
                                        Map<String, Object> param = f1.containsKey("parameters") ? (Map<String, Object>) f1.get("parameters") : null;
                                        sqlOperator.getColumnsType().put(((String) f1.get("field")).toLowerCase(), columnTypeMapper(fieldType, param));
                                    }
                                }

                                if (key.equals("costomColumns")) {
                                    for (Map<String, Object> f1 : f) {
                                        sqlOperator.getColumnsType().put("cus_" + f1.get("field"), columnTypeMapper((String) f1.get("type"), null));
                                    }
                                }

                            }

                        }
                    }
                }
                if (v.equals("payload")) {
                    Map<String, Object> m = (Map<String, Object>) msg.get(v);
                    for (String v1 : m.keySet()) {
                        Object o = m.get(v1);
                        if (o == null) {
                            continue;
                        }
                        if (v1.equals("key")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                sqlOperator.getPr().put(entry.getKey().toLowerCase(), entry.getValue());
                            }

                        }

                        if (v1.equals("before")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                //Object val = covertValue(sqlOperator.getColumnsType().get(entry.getKey().toLowerCase()), entry.getValue());
                                Object val = covertValue(sqlOperator, entry);
                                sqlOperator.getColumnsValue().put(entry.getKey().toLowerCase(), val);
                            }

                        }

                        if (v1.equals("after")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                //Object val = covertValue(sqlOperator.getColumnsType().get(entry.getKey().toLowerCase()), entry.getValue());
                                Object val = covertValue(sqlOperator, entry);
                                sqlOperator.getColumnsValue().put(entry.getKey().toLowerCase(), val);
                            }

                        }
                        if (v1.equals("costomColumns")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                sqlOperator.getColumnsValue().put("cus_" + entry.getKey(), entry.getValue());
                            }
                        }
                        if (v1.equals("source")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            sqlOperator.setDb((String) k.get("db"));
                            if (sqlOperator.getPr().size() < 1) {
                                sqlOperator.setTablename("npk_" + ((String) k.get("table")).toLowerCase());

                            } else {
                                sqlOperator.setTablename(((String) k.get("table")).toLowerCase());
                            }
                        }
                        if (v1.equals("op")) {
                            sqlOperator.setOpType((String) m.get("op"));
                        }
                        if (v1.equals("ts_ms")) {
                            sqlOperator.setOpts((Long) m.get("ts_ms"));
                        }

                    }
                }

            }

        }

        return sqlOperator;
    }
}
