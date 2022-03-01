package kafka.cli.producer.datagen;

import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.config.ConfigException;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

public enum Quickstart {
    CLICKSTREAM_CODES("cli/src/main/resources/clickstream_codes_schema.avro", "code"),
    CLICKSTREAM("cli/src/main/resources/clickstream_schema.avro", "ip"),
    CLICKSTREAM_USERS("cli/src/main/resources/clickstream_users_schema.avro", "user_id"),
    ORDERS("cli/src/main/resources/orders_schema.avro", "orderid"),
    RATINGS("cli/src/main/resources/ratings_schema.avro", "rating_id"),
    USERS("cli/src/main/resources/users_schema.avro", "userid"),
    USERS_("cli/src/main/resources/users_array_map_schema.avro", "userid"),
    PAGEVIEWS("cli/src/main/resources/pageviews_schema.avro", "viewtime"),
    STOCK_TRADES("cli/src/main/resources/stock_trades_schema.avro", "symbol"),
    INVENTORY("cli/src/main/resources/inventory.avro", "id"),
    PRODUCT("cli/src/main/resources/product.avro", "id"),
    PURCHASES("cli/src/main/resources/purchase.avro", "id"),
    TRANSACTIONS("cli/src/main/resources/transactions.avro", "transaction_id"),
    STORES("cli/src/main/resources/stores.avro", "store_id"),
    CREDIT_CARDS("cli/src/main/resources/credit_cards.avro", "card_id");

    static final Set<String> configValues = new HashSet<>();

    static {
        for (Quickstart q : Quickstart.values()) {
            configValues.add(q.name().toLowerCase());
        }
    }

    private final String schemaFilename;
    private final String keyName;

    Quickstart(String schemaFilename, String keyName) {
        this.schemaFilename = schemaFilename;
        this.keyName = keyName;
    }

    public InputStream getSchemaFilename() {
        try {
            return Quickstart.class.getClassLoader().getResourceAsStream(schemaFilename);
        } catch (SchemaParseException i) {
            // log.error("Unable to parse the provided schema", i);
            throw new ConfigException("Unable to parse the provided schema");
        }
    }

    public String getSchemaKeyField() {
        return keyName;
    }
}
