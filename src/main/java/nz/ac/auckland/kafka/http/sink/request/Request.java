package nz.ac.auckland.kafka.http.sink.request;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

public interface Request {

    Request setHeaders(String headers, String headerSeparator);
    void sendPayload(final Collection<SinkRecord> records);
}
