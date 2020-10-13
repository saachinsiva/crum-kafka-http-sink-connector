package nz.ac.auckland.kafka.http.sink.request;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

public class ApiRequest implements Request {

    static final String REQUEST_HEADER_CORRELATION_ID_KEY = "X-API-Correlation-Id";
    static final String REQUEST_HEADER_KAFKA_TOPIC_KEY = "X-Kafka-Topic";
    private HttpURLConnection connection;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private KafkaRecord kafkaRecord;
    private static final List<Integer> CALLBACK_API_DOWN_HTTP_STATUS_CODE = Arrays.asList(502, 503, 504);

    private final JsonConverter converter;
    private final ObjectMapper mapper;

    ApiRequest(HttpURLConnection connection, KafkaRecord kafkaRecord) {
        this.connection = connection;
        this.kafkaRecord = kafkaRecord;
        this.converter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        this.converter.configure(converterConfig, false);
        this.mapper = new ObjectMapper();
    }

    @Override
    public ApiRequest setHeaders(String headers, String headerSeparator) {
        log.debug("Processing headers: headerSeparator={}", headerSeparator);
        for (String headerKeyValue : headers.split(headerSeparator)) {
            if (headerKeyValue.contains(":")) {
                String key = headerKeyValue.split(":")[0];
                String value = headerKeyValue.split(":")[1];
                log.debug("Setting header property: {}", key);
                connection.setRequestProperty(key, value);
            }
        }
        addCorrelationIdHeader();
        addTopicHeader();
        return this;
    }

    private void addTopicHeader() {
        log.debug("Adding topic header: {} = {} ", REQUEST_HEADER_KAFKA_TOPIC_KEY, kafkaRecord.getTopic());
        connection.setRequestProperty(REQUEST_HEADER_KAFKA_TOPIC_KEY, kafkaRecord.getTopic());
    }

    private void addCorrelationIdHeader() {
        String correlationId = kafkaRecord.getTopic() + "-" + kafkaRecord.getOffset();
        log.debug("Adding correlationId header: {} = {} ", REQUEST_HEADER_CORRELATION_ID_KEY, correlationId);
        connection.setRequestProperty(REQUEST_HEADER_CORRELATION_ID_KEY, correlationId);
    }

    @Override
    public void sendPayload(SinkRecord record) {
        try (OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
                JsonGenerator generator = mapper.getFactory().createGenerator(out)) {
            if (record != null && record.value() != null && record.value() instanceof Struct) {
                byte[] rawData = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
                String valueInString = new String(rawData, StandardCharsets.UTF_8);
                generator.writeStartArray();
                generator.writeRawValue(valueInString);
                generator.writeEndArray();
                generator.flush();
                log.info("Submitted request: url={} payload={}", connection.getURL(), valueInString);
                isSendRequestSuccessful();
                validateResponse();
            }
        } catch (ApiResponseErrorException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            throw new ApiRequestErrorException(e.getLocalizedMessage(), kafkaRecord);
        } finally {
            connection.disconnect();
        }
    }

    private void isSendRequestSuccessful() {
        try {
            int statusCode = connection.getResponseCode();
            log.info("Response Status: {}", statusCode);
            if (CALLBACK_API_DOWN_HTTP_STATUS_CODE.contains(statusCode)) {
                throw new ApiRequestErrorException(
                        "Unable to connect to callback API: " + " received status: " + statusCode, kafkaRecord);
            }
        } catch (SocketTimeoutException e) {
            log.warn("Unable to obtain response from callback API. \n Error:{} ", e.getMessage());
            throw new ApiResponseErrorException(e.getLocalizedMessage());
        } catch (IOException e) {
            log.warn("Error checking if Send Request was Successful.");
            e.printStackTrace();
        }
    }

    private void validateResponse() {
        String resp = getResponse();
        log.info("Response {}", resp);
        try {
            JSONObject response = new JSONObject(resp);
            boolean retry = response.getBoolean("retry");
            if (retry) {
                throw new ApiResponseErrorException("Unable to process message.", kafkaRecord);
            }
        } catch (Exception e) {
            // it is not json so nothing to do here.
            log.info("Response is not JSON.");
        }
    }

    private String getResponse() {
        try {
            return readResponse(connection.getInputStream());
        } catch (IOException e) {
            try {
                String error = readResponse(connection.getErrorStream());
                log.error("Error Validating response. \n Error:{}", error);
                throw new ApiRequestErrorException(error, kafkaRecord);
            } catch (IOException e1) {
                log.error("Error Validating response. \n Error:{}", e1.getLocalizedMessage());
                throw new ApiRequestErrorException(e1.getLocalizedMessage(), kafkaRecord);
            }

        }
    }

    private String readResponse(InputStream stream) throws IOException {
        StringBuilder sb;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            String response = sb.toString();
            log.info("Response:{}", response);
            return response;
        }
    }
}
