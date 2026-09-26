package it.kristikomini.shipment.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/** JSON (de)serialization for event payloads. */
@Component
public class EventJson {

    private final ObjectMapper mapper;

    public EventJson(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String toJson(Object event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize event " + event, e);
        }
    }

    public <T> T fromJson(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot deserialize " + type.getSimpleName(), e);
        }
    }
}
