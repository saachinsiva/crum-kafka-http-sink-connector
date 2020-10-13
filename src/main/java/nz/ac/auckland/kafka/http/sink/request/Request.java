package nz.ac.auckland.kafka.http.sink.request;

import org.apache.kafka.connect.sink.SinkRecord;

public interface Request {

    Request setHeaders(String headers, String headerSeparator);
    void sendPayload(SinkRecord payload);
}
