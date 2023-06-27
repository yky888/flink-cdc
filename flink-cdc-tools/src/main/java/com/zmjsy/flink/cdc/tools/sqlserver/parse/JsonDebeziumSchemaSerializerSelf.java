//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.zmjsy.flink.cdc.tools.sqlserver.parse;

import com.zmjsy.flink.cdc.tools.cdc.SourceSchema;
import com.zmjsy.flink.cdc.tools.doris.FieldSchema;
import com.zmjsy.flink.cdc.tools.sqlserver.consts.DorisMetadataConsts;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.HttpGetWithEntity;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.doris.shaded.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.shaded.org.apache.commons.codec.binary.Base64;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonDebeziumSchemaSerializerSelf implements DorisRecordSerializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaSerializerSelf.class);
    private static final String CHECK_SCHEMA_CHANGE_API = "http://%s/api/enable_light_schema_change/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String OP_READ = "r";
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s";
    private static final String addDropDDLRegex = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    private final Pattern addDropDDLPattern;
    private DorisOptions dorisOptions;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String database;
    private String table;
    private String sourceTableName;

    private SourceSchema schema;

    private int utc;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public JsonDebeziumSchemaSerializerSelf(DorisOptions dorisOptions, Pattern pattern, String sourceTableName, SourceSchema schema,int utc) {
        this.dorisOptions = dorisOptions;
        this.addDropDDLPattern = pattern == null ? Pattern.compile("ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*", 2) : pattern;
        String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
        this.database = tableInfo[0];
        this.table = tableInfo[1];
        this.sourceTableName = sourceTableName;
        this.schema = schema;
        this.utc=utc;
    }

    public byte[] serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = this.objectMapper.readTree(record);
        String op = this.extractJsonNode(recordRoot, "op");
        if (Objects.isNull(op)) {
            this.schemaChange(recordRoot);
            return null;
        } else {
            Map valueMap;
            if (!"r".equals(op) && !"c".equals(op)) {
                if ("u".equals(op)) {
                    valueMap = this.extractAfterRow(recordRoot);
                    this.addDeleteSign(valueMap, false);
                } else {
                    if (!"d".equals(op)) {
                        LOG.error("parse record fail, unknown op {} in {}", op, record);
                        return null;
                    }

                    valueMap = this.extractBeforeRow(recordRoot);
                    this.addDeleteSign(valueMap, true);
                }
            } else {
                valueMap = this.extractAfterRow(recordRoot);
                this.addDeleteSign(valueMap, false);
            }
            LinkedHashMap<String, FieldSchema> fields = schema.getFields();

            //大写转小写
            HashMap<String, Object> resMap = new HashMap<>();

            //增加数据写入时间
            String format = sdf.format(new Date());
            resMap.put("etl_time",format);
            resMap.put("__DORIS_DELETE_SIGN__",valueMap.get("__DORIS_DELETE_SIGN__")+"");

            for(Map.Entry<String, FieldSchema> entrySet :fields.entrySet()){
                String columnType = entrySet.getValue().getTypeString().toLowerCase();
                String columnName = entrySet.getKey();
                Object res = valueMap.get(columnName);
                if(DorisMetadataConsts.DATE_TYPE_LIST.contains(columnType)){
                    if(res==null || "null".equals(res)){
                        continue;
                    }
                    Long time = Long.valueOf(res + "");
                    res = sdf.format(new Date(time-(3600000*utc)));
                }
                resMap.put(columnName.toLowerCase(),res);
            }
            return this.objectMapper.writeValueAsString(resMap).getBytes(StandardCharsets.UTF_8);
        }
    }

    @VisibleForTesting
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;

        try {
            if (!StringUtils.isNullOrWhitespaceOnly(this.sourceTableName) && !this.checkTable(recordRoot)) {
                return false;
            }

            String ddl = this.extractDDL(recordRoot);
            if (StringUtils.isNullOrWhitespaceOnly(ddl)) {
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }

            boolean doSchemaChange = this.checkSchemaChange(ddl);
            status = doSchemaChange && this.execSchemaChange(ddl);
            LOG.info("schema change status:{}", status);
        } catch (Exception var5) {
            LOG.warn("schema change error :", var5);
        }

        return status;
    }

    protected boolean checkTable(JsonNode recordRoot) {
        String db = this.extractDatabase(recordRoot);
        String tbl = this.extractTable(recordRoot);
        String dbTbl = db + "." + tbl;
        return this.sourceTableName.equals(dbTbl);
    }

    private void addDeleteSign(Map<String, String> valueMap, boolean delete) {
        if (delete) {
            valueMap.put("__DORIS_DELETE_SIGN__", "1");
        } else {
            valueMap.put("__DORIS_DELETE_SIGN__", "0");
        }

    }

    private boolean checkSchemaChange(String ddl) throws IOException {
        String requestUrl = String.format("http://%s/api/enable_light_schema_change/%s/%s", this.dorisOptions.getFenodes(), this.database, this.table);
        Map<String, Object> param = this.buildRequestParam(ddl);
        if (param.size() != 2) {
            return false;
        } else {
            HttpGetWithEntity httpGet = new HttpGetWithEntity(requestUrl);
            httpGet.setHeader("Authorization", this.authHeader());
            httpGet.setEntity(new StringEntity(this.objectMapper.writeValueAsString(param)));
            boolean success = this.handleResponse(httpGet);
            if (!success) {
                LOG.warn("schema change can not do table {}.{}", this.database, this.table);
            }

            return success;
        }
    }

    protected Map<String, Object> buildRequestParam(String ddl) {
        Map<String, Object> params = new HashMap();
        Matcher matcher = this.addDropDDLPattern.matcher(ddl);
        if (matcher.find()) {
            String op = matcher.group(1);
            String col = matcher.group(3);
            params.put("isDropColumn", op.equalsIgnoreCase("DROP"));
            params.put("columnName", col);
        }

        return params;
    }

    private boolean execSchemaChange(String ddl) throws IOException {
        Map<String, String> param = new HashMap();
        param.put("stmt", ddl);
        String requestUrl = String.format("http://%s/api/query/default_cluster/%s", this.dorisOptions.getFenodes(), this.database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader("Authorization", this.authHeader());
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(this.objectMapper.writeValueAsString(param)));
        boolean success = this.handleResponse(httpPost);
        return success;
    }

    protected String extractDatabase(JsonNode record) {
        return record.get("source").has("schema") ? this.extractJsonNode(record.get("source"), "schema") : this.extractJsonNode(record.get("source"), "db");
    }

    protected String extractTable(JsonNode record) {
        return this.extractJsonNode(record.get("source"), "table");
    }

    private boolean handleResponse(HttpUriRequest request) {
        try {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            Throwable var3 = null;

            boolean var9;
            try {
                CloseableHttpResponse response = httpclient.execute(request);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200 || response.getEntity() == null) {
                    return false;
                }

                String loadResult = EntityUtils.toString(response.getEntity());
                Map<String, Object> responseMap = (Map)this.objectMapper.readValue(loadResult, Map.class);
                String code = responseMap.getOrDefault("code", "-1").toString();
                if (!code.equals("0")) {
                    LOG.error("schema change response:{}", loadResult);
                    return false;
                }

                var9 = true;
            } catch (Throwable var20) {
                var3 = var20;
                throw var20;
            } finally {
                if (httpclient != null) {
                    if (var3 != null) {
                        try {
                            httpclient.close();
                        } catch (Throwable var19) {
                            var3.addSuppressed(var19);
                        }
                    } else {
                        httpclient.close();
                    }
                }

            }

            return var9;
        } catch (Exception var22) {
            LOG.error("http request error,", var22);
            return false;
        }
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private Map<String, String> extractBeforeRow(JsonNode record) {
        return this.extractRow(record.get("before"));
    }

    private Map<String, String> extractAfterRow(JsonNode record) {
        return this.extractRow(record.get("after"));
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        Map<String, String> recordMap = (Map)this.objectMapper.convertValue(recordRow, new TypeReference<Map<String, String>>() {
        });
        return (Map)(recordMap != null ? recordMap : new HashMap());
    }

    public String extractDDL(JsonNode record) throws JsonProcessingException {
        String historyRecord = this.extractJsonNode(record, "historyRecord");
        if (Objects.isNull(historyRecord)) {
            return null;
        } else {
            String ddl = this.extractJsonNode(this.objectMapper.readTree(historyRecord), "ddl");
            LOG.debug("received debezium ddl :{}", ddl);
            if (!Objects.isNull(ddl)) {
                Matcher matcher = this.addDropDDLPattern.matcher(ddl);
                if (matcher.find()) {
                    String op = matcher.group(1);
                    String col = matcher.group(3);
                    String type = matcher.group(5);
                    type = type == null ? "" : type;
                    ddl = String.format("ALTER TABLE %s %s COLUMN %s %s", this.dorisOptions.getTableIdentifier(), op, col, type);
                    LOG.info("parse ddl:{}", ddl);
                    return ddl;
                }
            }

            return null;
        }
    }

    private String authHeader() {
        return "Basic " + new String(Base64.encodeBase64((this.dorisOptions.getUsername() + ":" + this.dorisOptions.getPassword()).getBytes(StandardCharsets.UTF_8)));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private DorisOptions dorisOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;
        private SourceSchema schema;
        private int utc;

        public Builder setUtc(int utc) {
            this.utc = utc;
            return this;
        }

        public Builder setSourceSchema(SourceSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder() {
        }

        public Builder setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public Builder setPattern(Pattern addDropDDLPattern) {
            this.addDropDDLPattern = addDropDDLPattern;
            return this;
        }

        public Builder setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
            return this;
        }

        public JsonDebeziumSchemaSerializerSelf build() {
            return new JsonDebeziumSchemaSerializerSelf(this.dorisOptions, this.addDropDDLPattern, this.sourceTableName,this.schema,this.utc);
        }
    }
}
