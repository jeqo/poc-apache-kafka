/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package poc.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

@org.apache.avro.specific.AvroGenerated
public class User
  extends org.apache.avro.specific.SpecificRecordBase
  implements org.apache.avro.specific.SpecificRecord {

  private static final long serialVersionUID = -7980015867623841848L;

  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
    .parse(
      "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"poc.avro\",\"fields\":[{\"name\":\"username\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"create_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"country\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}"
    );

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  private static final SpecificData MODEL$ = new SpecificData();

  static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
  }

  private static final BinaryMessageEncoder<User> ENCODER = new BinaryMessageEncoder<User>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<User> DECODER = new BinaryMessageDecoder<User>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<User> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<User> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<User> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<User>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this User to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a User from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a User instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static User fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.String username;
  private java.time.Instant create_at;
  private java.lang.String country;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public User() {}

  /**
   * All-args constructor.
   * @param username The new value for username
   * @param create_at The new value for create_at
   * @param country The new value for country
   */
  public User(java.lang.String username, java.time.Instant create_at, java.lang.String country) {
    this.username = username;
    this.create_at = create_at.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    this.country = country;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() {
    return MODEL$;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
      case 0:
        return username;
      case 1:
        return create_at;
      case 2:
        return country;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  private static final org.apache.avro.Conversion<?>[] conversions = new org.apache.avro.Conversion<?>[] {
    null,
    new org.apache.avro.data.TimeConversions.TimestampMillisConversion(),
    null,
    null,
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        username = value$ != null ? value$.toString() : null;
        break;
      case 1:
        create_at = (java.time.Instant) value$;
        break;
      case 2:
        country = value$ != null ? value$.toString() : null;
        break;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'username' field.
   * @return The value of the 'username' field.
   */
  public java.lang.String getUsername() {
    return username;
  }

  /**
   * Sets the value of the 'username' field.
   * @param value the value to set.
   */
  public void setUsername(java.lang.String value) {
    this.username = value;
  }

  /**
   * Gets the value of the 'create_at' field.
   * @return The value of the 'create_at' field.
   */
  public java.time.Instant getCreateAt() {
    return create_at;
  }

  /**
   * Sets the value of the 'create_at' field.
   * @param value the value to set.
   */
  public void setCreateAt(java.time.Instant value) {
    this.create_at = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
  }

  /**
   * Gets the value of the 'country' field.
   * @return The value of the 'country' field.
   */
  public java.lang.String getCountry() {
    return country;
  }

  /**
   * Sets the value of the 'country' field.
   * @param value the value to set.
   */
  public void setCountry(java.lang.String value) {
    this.country = value;
  }

  /**
   * Creates a new User RecordBuilder.
   * @return A new User RecordBuilder
   */
  public static poc.avro.User.Builder newBuilder() {
    return new poc.avro.User.Builder();
  }

  /**
   * Creates a new User RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new User RecordBuilder
   */
  public static poc.avro.User.Builder newBuilder(poc.avro.User.Builder other) {
    if (other == null) {
      return new poc.avro.User.Builder();
    } else {
      return new poc.avro.User.Builder(other);
    }
  }

  /**
   * Creates a new User RecordBuilder by copying an existing User instance.
   * @param other The existing instance to copy.
   * @return A new User RecordBuilder
   */
  public static poc.avro.User.Builder newBuilder(poc.avro.User other) {
    if (other == null) {
      return new poc.avro.User.Builder();
    } else {
      return new poc.avro.User.Builder(other);
    }
  }

  /**
   * RecordBuilder for User instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder
    extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
    implements org.apache.avro.data.RecordBuilder<User> {

    private java.lang.String username;
    private java.time.Instant create_at;
    private java.lang.String country;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(poc.avro.User.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.username)) {
        this.username = data().deepCopy(fields()[0].schema(), other.username);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.create_at)) {
        this.create_at = data().deepCopy(fields()[1].schema(), other.create_at);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.country)) {
        this.country = data().deepCopy(fields()[2].schema(), other.country);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing User instance
     * @param other The existing instance to copy.
     */
    private Builder(poc.avro.User other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.username)) {
        this.username = data().deepCopy(fields()[0].schema(), other.username);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.create_at)) {
        this.create_at = data().deepCopy(fields()[1].schema(), other.create_at);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.country)) {
        this.country = data().deepCopy(fields()[2].schema(), other.country);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Gets the value of the 'username' field.
     * @return The value.
     */
    public java.lang.String getUsername() {
      return username;
    }

    /**
     * Sets the value of the 'username' field.
     * @param value The value of 'username'.
     * @return This builder.
     */
    public poc.avro.User.Builder setUsername(java.lang.String value) {
      validate(fields()[0], value);
      this.username = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'username' field has been set.
     * @return True if the 'username' field has been set, false otherwise.
     */
    public boolean hasUsername() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'username' field.
     * @return This builder.
     */
    public poc.avro.User.Builder clearUsername() {
      username = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'create_at' field.
     * @return The value.
     */
    public java.time.Instant getCreateAt() {
      return create_at;
    }

    /**
     * Sets the value of the 'create_at' field.
     * @param value The value of 'create_at'.
     * @return This builder.
     */
    public poc.avro.User.Builder setCreateAt(java.time.Instant value) {
      validate(fields()[1], value);
      this.create_at = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
     * Checks whether the 'create_at' field has been set.
     * @return True if the 'create_at' field has been set, false otherwise.
     */
    public boolean hasCreateAt() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'create_at' field.
     * @return This builder.
     */
    public poc.avro.User.Builder clearCreateAt() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
     * Gets the value of the 'country' field.
     * @return The value.
     */
    public java.lang.String getCountry() {
      return country;
    }

    /**
     * Sets the value of the 'country' field.
     * @param value The value of 'country'.
     * @return This builder.
     */
    public poc.avro.User.Builder setCountry(java.lang.String value) {
      validate(fields()[2], value);
      this.country = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
     * Checks whether the 'country' field has been set.
     * @return True if the 'country' field has been set, false otherwise.
     */
    public boolean hasCountry() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'country' field.
     * @return This builder.
     */
    public poc.avro.User.Builder clearCountry() {
      country = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public User build() {
      try {
        User record = new User();
        record.username = fieldSetFlags()[0] ? this.username : (java.lang.String) defaultValue(fields()[0]);
        record.create_at = fieldSetFlags()[1] ? this.create_at : (java.time.Instant) defaultValue(fields()[1]);
        record.country = fieldSetFlags()[2] ? this.country : (java.lang.String) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<User> WRITER$ = (org.apache.avro.io.DatumWriter<User>) MODEL$.createDatumWriter(
    SCHEMA$
  );

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<User> READER$ = (org.apache.avro.io.DatumReader<User>) MODEL$.createDatumReader(
    SCHEMA$
  );

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }
}
