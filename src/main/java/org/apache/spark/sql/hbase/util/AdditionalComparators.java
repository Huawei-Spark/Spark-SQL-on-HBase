/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase.util;

public final class AdditionalComparators {

  private AdditionalComparators() {}
  public static void registerAllExtensions(
          com.google.protobuf.ExtensionRegistry registry) {
  }
  
  public interface CustomComparatorOrBuilder
          extends com.google.protobuf.MessageOrBuilder {

    // required .ByteArrayComparable comparable = 1;
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    boolean hasComparable();
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable getComparable();
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder getComparableOrBuilder();
  }

  public static final class CustomComparator extends
          com.google.protobuf.GeneratedMessage
          implements CustomComparatorOrBuilder {
    // Use CustomComparator.newBuilder() to construct.
    private CustomComparator(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private CustomComparator(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final CustomComparator defaultInstance;
    public static CustomComparator getDefaultInstance() {
      return defaultInstance;
    }

    public CustomComparator getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private CustomComparator(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
              com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                      extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) == 0x00000001)) {
                subBuilder = comparable_.toBuilder();
              }
              comparable_ = input.readMessage(org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(comparable_);
                comparable_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
                e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
    getDescriptor() {
      return internal_static_CustomComparator_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
    internalGetFieldAccessorTable() {
      return internal_static_CustomComparator_fieldAccessorTable
              .ensureFieldAccessorsInitialized(
                      CustomComparator.class, CustomComparator.Builder.class);
    }

    public static com.google.protobuf.Parser<CustomComparator> PARSER =
            new com.google.protobuf.AbstractParser<CustomComparator>() {
              public CustomComparator parsePartialFrom(
                      com.google.protobuf.CodedInputStream input,
                      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                      throws com.google.protobuf.InvalidProtocolBufferException {
                return new CustomComparator(input, extensionRegistry);
              }
            };

    @java.lang.Override
    public com.google.protobuf.Parser<CustomComparator> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required .ByteArrayComparable comparable = 1;
    public static final int COMPARABLE_FIELD_NUMBER = 1;
    private org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable comparable_;
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    public boolean hasComparable() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    public org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable getComparable() {
      return comparable_;
    }
    /**
     * <code>required .ByteArrayComparable comparable = 1;</code>
     */
    public org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder getComparableOrBuilder() {
      return comparable_;
    }

    private void initFields() {
      comparable_ = org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.getDefaultInstance();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasComparable()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
            throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeMessage(1, comparable_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
                .computeMessageSize(1, comparable_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
            throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof CustomComparator)) {
        return super.equals(obj);
      }
      CustomComparator other = (CustomComparator) obj;

      boolean result = true;
      result = result && (hasComparable() == other.hasComparable());
      if (hasComparable()) {
        result = result && getComparable()
                .equals(other.getComparable());
      }
      result = result &&
              getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasComparable()) {
        hash = (37 * hash) + COMPARABLE_FIELD_NUMBER;
        hash = (53 * hash) + getComparable().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static CustomComparator parseFrom(
            com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static CustomComparator parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static CustomComparator parseFrom(byte[] data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static CustomComparator parseFrom(
            byte[] data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static CustomComparator parseFrom(java.io.InputStream input)
            throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static CustomComparator parseFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static CustomComparator parseDelimitedFrom(java.io.InputStream input)
            throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static CustomComparator parseDelimitedFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static CustomComparator parseFrom(
            com.google.protobuf.CodedInputStream input)
            throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static CustomComparator parseFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(CustomComparator prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
            com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code CustomComparator}
     */
    public static final class Builder extends
            com.google.protobuf.GeneratedMessage.Builder<Builder>
            implements CustomComparatorOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
        return internal_static_CustomComparator_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
        return internal_static_CustomComparator_fieldAccessorTable
                .ensureFieldAccessorsInitialized(
                        CustomComparator.class, CustomComparator.Builder.class);
      }

      // Construct using CustomComparator.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
              com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getComparableFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (comparableBuilder_ == null) {
          comparable_ = org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.getDefaultInstance();
        } else {
          comparableBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
      getDescriptorForType() {
        return internal_static_CustomComparator_descriptor;
      }

      public CustomComparator getDefaultInstanceForType() {
        return CustomComparator.getDefaultInstance();
      }

      public CustomComparator build() {
        CustomComparator result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public CustomComparator buildPartial() {
        CustomComparator result = new CustomComparator(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        if (comparableBuilder_ == null) {
          result.comparable_ = comparable_;
        } else {
          result.comparable_ = comparableBuilder_.build();
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof CustomComparator) {
          return mergeFrom((CustomComparator)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(CustomComparator other) {
        if (other == CustomComparator.getDefaultInstance()) return this;
        if (other.hasComparable()) {
          mergeComparable(other.getComparable());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasComparable()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws java.io.IOException {
        CustomComparator parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (CustomComparator) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required .ByteArrayComparable comparable = 1;
      private org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable comparable_ = org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.getDefaultInstance();
      private com.google.protobuf.SingleFieldBuilder<
              org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder> comparableBuilder_;
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public boolean hasComparable() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable getComparable() {
        if (comparableBuilder_ == null) {
          return comparable_;
        } else {
          return comparableBuilder_.getMessage();
        }
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public Builder setComparable(org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable value) {
        if (comparableBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          comparable_ = value;
          onChanged();
        } else {
          comparableBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public Builder setComparable(
              org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder builderForValue) {
        if (comparableBuilder_ == null) {
          comparable_ = builderForValue.build();
          onChanged();
        } else {
          comparableBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public Builder mergeComparable(org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable value) {
        if (comparableBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001) &&
                  comparable_ != org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.getDefaultInstance()) {
            comparable_ =
                    org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.newBuilder(comparable_).mergeFrom(value).buildPartial();
          } else {
            comparable_ = value;
          }
          onChanged();
        } else {
          comparableBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public Builder clearComparable() {
        if (comparableBuilder_ == null) {
          comparable_ = org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.getDefaultInstance();
          onChanged();
        } else {
          comparableBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder getComparableBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getComparableFieldBuilder().getBuilder();
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      public org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder getComparableOrBuilder() {
        if (comparableBuilder_ != null) {
          return comparableBuilder_.getMessageOrBuilder();
        } else {
          return comparable_;
        }
      }
      /**
       * <code>required .ByteArrayComparable comparable = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
              org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder>
      getComparableFieldBuilder() {
        if (comparableBuilder_ == null) {
          comparableBuilder_ = new com.google.protobuf.SingleFieldBuilder<
                  org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable.Builder, org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparableOrBuilder>(
                  comparable_,
                  getParentForChildren(),
                  isClean());
          comparable_ = null;
        }
        return comparableBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:CustomComparator)
    }

    static {
      defaultInstance = new CustomComparator(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:CustomComparator)
  }

  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_Comparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_Comparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_ByteArrayComparable_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_ByteArrayComparable_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_CustomComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_CustomComparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_BinaryPrefixComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_BinaryPrefixComparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_BitComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_BitComparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_NullComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_NullComparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_RegexStringComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_RegexStringComparator_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
          internal_static_SubstringComparator_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_SubstringComparator_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
  getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
          descriptor;
  static {
    java.lang.String[] descriptorData = {
            "\n\020Comparator.proto\"9\n\nComparator\022\014\n\004name" +
                    "\030\001 \002(\t\022\035\n\025serialized_comparator\030\002 \001(\014\"$\n" +
                    "\023ByteArrayComparable\022\r\n\005value\030\001 \001(\014\"<\n\020B" +
                    "inaryComparator\022(\n\ncomparable\030\001 \002(\0132\024.By" +
                    "teArrayComparable\"B\n\026BinaryPrefixCompara" +
                    "tor\022(\n\ncomparable\030\001 \002(\0132\024.ByteArrayCompa" +
                    "rable\"\216\001\n\rBitComparator\022(\n\ncomparable\030\001 " +
                    "\002(\0132\024.ByteArrayComparable\022,\n\nbitwise_op\030" +
                    "\002 \002(\0162\030.BitComparator.BitwiseOp\"%\n\tBitwi" +
                    "seOp\022\007\n\003AND\020\001\022\006\n\002OR\020\002\022\007\n\003XOR\020\003\"\020\n\016NullCo",
            "mparator\"P\n\025RegexStringComparator\022\017\n\007pat" +
                    "tern\030\001 \002(\t\022\025\n\rpattern_flags\030\002 \002(\005\022\017\n\007cha" +
                    "rset\030\003 \002(\t\"%\n\023SubstringComparator\022\016\n\006sub" +
                    "str\030\001 \002(\tBF\n*org.apache.hadoop.hbase.pro" +
                    "tobuf.generatedB\020ComparatorProtosH\001\210\001\001\240\001" +
                    "\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
            new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
              public com.google.protobuf.ExtensionRegistry assignDescriptors(
                      com.google.protobuf.Descriptors.FileDescriptor root) {
                descriptor = root;
                internal_static_Comparator_descriptor =
                        getDescriptor().getMessageTypes().get(0);
                internal_static_Comparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_Comparator_descriptor,
                        new java.lang.String[] { "Name", "SerializedComparator", });
                internal_static_ByteArrayComparable_descriptor =
                        getDescriptor().getMessageTypes().get(1);
                internal_static_ByteArrayComparable_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_ByteArrayComparable_descriptor,
                        new java.lang.String[] { "Value", });
                internal_static_CustomComparator_descriptor =
                        getDescriptor().getMessageTypes().get(2);
                internal_static_CustomComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_CustomComparator_descriptor,
                        new java.lang.String[] { "Comparable", });
                internal_static_BinaryPrefixComparator_descriptor =
                        getDescriptor().getMessageTypes().get(3);
                internal_static_BinaryPrefixComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_BinaryPrefixComparator_descriptor,
                        new java.lang.String[] { "Comparable", });
                internal_static_BitComparator_descriptor =
                        getDescriptor().getMessageTypes().get(4);
                internal_static_BitComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_BitComparator_descriptor,
                        new java.lang.String[] { "Comparable", "BitwiseOp", });
                internal_static_NullComparator_descriptor =
                        getDescriptor().getMessageTypes().get(5);
                internal_static_NullComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_NullComparator_descriptor,
                        new java.lang.String[] { });
                internal_static_RegexStringComparator_descriptor =
                        getDescriptor().getMessageTypes().get(6);
                internal_static_RegexStringComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_RegexStringComparator_descriptor,
                        new java.lang.String[] { "Pattern", "PatternFlags", "Charset", });
                internal_static_SubstringComparator_descriptor =
                        getDescriptor().getMessageTypes().get(7);
                internal_static_SubstringComparator_fieldAccessorTable = new
                        com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                        internal_static_SubstringComparator_descriptor,
                        new java.lang.String[] { "Substr", });
                return null;
              }
            };
    com.google.protobuf.Descriptors.FileDescriptor
            .internalBuildGeneratedFileFrom(descriptorData,
                    new com.google.protobuf.Descriptors.FileDescriptor[] {
                    }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
