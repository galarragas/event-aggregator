/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.pragmasoft.eventaggregator.support.data;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class IntegrationProfileCreated extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ =
          new org.apache.avro.Schema.Parser().parse(
                  "{\"type\":\"record\",\"name\":" +
                          "\"IntegrationProfileCreated\",\"namespace\":\"com.pragmasoft.eventaggregator\"," +
                          "\"fields\":[" +
                                "{\"name\":\"header\",\"type\":{\"type\":\"record\",\"name\":\"IntegrationEventHeader\",\"fields\":[" +
                                    "{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}," +
                                    "{\"name\":\"correlationId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}," +
                                    "{\"name\":\"eventTs\",\"type\":\"long\"}" +
                                "]}}," +
                                "{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}," +
                                "{\"name\":\"firstName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}," +
                                "{\"name\":\"lastName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}," +
                                "{\"name\":\"username\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}" +
                            "]}");

  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public IntegrationEventHeader header;
  @Deprecated public String userId;
  @Deprecated public String firstName;
  @Deprecated public String lastName;
  @Deprecated public String username;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public IntegrationProfileCreated() {}

  /**
   * All-args constructor.
   */
  public IntegrationProfileCreated(IntegrationEventHeader header, String userId, String firstName, String lastName, String username) {
    this.header = header;
    this.userId = userId;
    this.firstName = firstName;
    this.lastName = lastName;
    this.username = username;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return header;
    case 1: return userId;
    case 2: return firstName;
    case 3: return lastName;
    case 4: return username;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: header = (IntegrationEventHeader)value$; break;
    case 1: userId = (String)value$; break;
    case 2: firstName = (String)value$; break;
    case 3: lastName = (String)value$; break;
    case 4: username = (String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'header' field.
   */
  public IntegrationEventHeader getHeader() {
    return header;
  }

  /**
   * Sets the value of the 'header' field.
   * @param value the value to set.
   */
  public void setHeader(IntegrationEventHeader value) {
    this.header = value;
  }

  /**
   * Gets the value of the 'userId' field.
   */
  public String getUserId() {
    return userId;
  }

  /**
   * Sets the value of the 'userId' field.
   * @param value the value to set.
   */
  public void setUserId(String value) {
    this.userId = value;
  }

  /**
   * Gets the value of the 'firstName' field.
   */
  public String getFirstName() {
    return firstName;
  }

  /**
   * Sets the value of the 'firstName' field.
   * @param value the value to set.
   */
  public void setFirstName(String value) {
    this.firstName = value;
  }

  /**
   * Gets the value of the 'lastName' field.
   */
  public String getLastName() {
    return lastName;
  }

  /**
   * Sets the value of the 'lastName' field.
   * @param value the value to set.
   */
  public void setLastName(String value) {
    this.lastName = value;
  }

  /**
   * Gets the value of the 'username' field.
   */
  public String getUsername() {
    return username;
  }

  /**
   * Sets the value of the 'username' field.
   * @param value the value to set.
   */
  public void setUsername(String value) {
    this.username = value;
  }



  /** Creates a new IntegrationProfileCreated RecordBuilder */
  public static IntegrationProfileCreated.Builder newBuilder() {
    return new IntegrationProfileCreated.Builder();
  }

  /** Creates a new IntegrationProfileCreated RecordBuilder by copying an existing Builder */
  public static IntegrationProfileCreated.Builder newBuilder(IntegrationProfileCreated.Builder other) {
    return new IntegrationProfileCreated.Builder(other);
  }

  /** Creates a new IntegrationProfileCreated RecordBuilder by copying an existing IntegrationProfileCreated instance */
  public static IntegrationProfileCreated.Builder newBuilder(IntegrationProfileCreated other) {
    return new IntegrationProfileCreated.Builder(other);
  }

  /**
   * RecordBuilder for IntegrationProfileCreated instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<IntegrationProfileCreated>
    implements org.apache.avro.data.RecordBuilder<IntegrationProfileCreated> {

    private IntegrationEventHeader header;
    private String userId;
    private String firstName;
    private String lastName;
    private String username;

    /** Creates a new Builder */
    private Builder() {
      super(IntegrationProfileCreated.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.header)) {
        this.header = data().deepCopy(fields()[0].schema(), other.header);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.userId)) {
        this.userId = data().deepCopy(fields()[1].schema(), other.userId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.firstName)) {
        this.firstName = data().deepCopy(fields()[2].schema(), other.firstName);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.lastName)) {
        this.lastName = data().deepCopy(fields()[3].schema(), other.lastName);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.username)) {
        this.username = data().deepCopy(fields()[4].schema(), other.username);
        fieldSetFlags()[4] = true;
      }
    }

    /** Creates a Builder by copying an existing IntegrationProfileCreated instance */
    private Builder(IntegrationProfileCreated other) {
            super(IntegrationProfileCreated.SCHEMA$);
      if (isValidValue(fields()[0], other.header)) {
        this.header = data().deepCopy(fields()[0].schema(), other.header);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.userId)) {
        this.userId = data().deepCopy(fields()[1].schema(), other.userId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.firstName)) {
        this.firstName = data().deepCopy(fields()[2].schema(), other.firstName);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.lastName)) {
        this.lastName = data().deepCopy(fields()[3].schema(), other.lastName);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.username)) {
        this.username = data().deepCopy(fields()[4].schema(), other.username);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'header' field */
    public IntegrationEventHeader getHeader() {
      return header;
    }

    /** Sets the value of the 'header' field */
    public IntegrationProfileCreated.Builder setHeader(IntegrationEventHeader value) {
      validate(fields()[0], value);
      this.header = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'header' field has been set */
    public boolean hasHeader() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'header' field */
    public IntegrationProfileCreated.Builder clearHeader() {
      header = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'userId' field */
    public String getUserId() {
      return userId;
    }

    /** Sets the value of the 'userId' field */
    public IntegrationProfileCreated.Builder setUserId(String value) {
      validate(fields()[1], value);
      this.userId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /** Checks whether the 'userId' field has been set */
    public boolean hasUserId() {
      return fieldSetFlags()[1];
    }

    /** Clears the value of the 'userId' field */
    public IntegrationProfileCreated.Builder clearUserId() {
      userId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'firstName' field */
    public String getFirstName() {
      return firstName;
    }

    /** Sets the value of the 'firstName' field */
    public IntegrationProfileCreated.Builder setFirstName(String value) {
      validate(fields()[2], value);
      this.firstName = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /** Checks whether the 'firstName' field has been set */
    public boolean hasFirstName() {
      return fieldSetFlags()[2];
    }

    /** Clears the value of the 'firstName' field */
    public IntegrationProfileCreated.Builder clearFirstName() {
      firstName = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'lastName' field */
    public String getLastName() {
      return lastName;
    }

    /** Sets the value of the 'lastName' field */
    public IntegrationProfileCreated.Builder setLastName(String value) {
      validate(fields()[3], value);
      this.lastName = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /** Checks whether the 'lastName' field has been set */
    public boolean hasLastName() {
      return fieldSetFlags()[3];
    }

    /** Clears the value of the 'lastName' field */
    public IntegrationProfileCreated.Builder clearLastName() {
      lastName = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'username' field */
    public String getUsername() {
      return username;
    }

    /** Sets the value of the 'username' field */
    public IntegrationProfileCreated.Builder setUsername(String value) {
      validate(fields()[4], value);
      this.username = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /** Checks whether the 'username' field has been set */
    public boolean hasUsername() {
      return fieldSetFlags()[4];
    }

    /** Clears the value of the 'username' field */
    public IntegrationProfileCreated.Builder clearUsername() {
      username = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public IntegrationProfileCreated build() {
      try {
        IntegrationProfileCreated record = new IntegrationProfileCreated();
        record.header = fieldSetFlags()[0] ? this.header : (IntegrationEventHeader) defaultValue(fields()[0]);
        record.userId = fieldSetFlags()[1] ? this.userId : (String) defaultValue(fields()[1]);
        record.firstName = fieldSetFlags()[2] ? this.firstName : (String) defaultValue(fields()[2]);
        record.lastName = fieldSetFlags()[3] ? this.lastName : (String) defaultValue(fields()[3]);
        record.username = fieldSetFlags()[4] ? this.username : (String) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}