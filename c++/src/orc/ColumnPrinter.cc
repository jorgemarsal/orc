/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/Adaptor.hh"
#include "orc/ColumnPrinter.hh"
#include "orc/orc-config.hh"

#include <limits>
#include <sstream>
#include <stdexcept>
#include <time.h>
#include <typeinfo>

#ifdef __clang__
  #pragma clang diagnostic ignored "-Wformat-security"
#endif

namespace orc {

  class BooleanColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    BooleanColumnPrinter(std::string&, const Type&);
    ~BooleanColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    LongColumnPrinter(std::string&, const Type&);
    ~LongColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
    const bool isFloat;

  public:
    DoubleColumnPrinter(std::string&, const Type&);
    virtual ~DoubleColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class TimestampColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    time_t epoch;

  public:
    TimestampColumnPrinter(std::string&, const Type&);
    ~TimestampColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DateColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;

  public:
    DateColumnPrinter(std::string&, const Type& type);
    ~DateColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal64ColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    int32_t scale;
  public:
    Decimal64ColumnPrinter(std::string&, const Type& type);
    ~Decimal64ColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal128ColumnPrinter: public ColumnPrinter {
  private:
    const Int128* data;
    int32_t scale;
  public:
    Decimal128ColumnPrinter(std::string&, const Type& type);
    ~Decimal128ColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    StringColumnPrinter(std::string&, const Type& type);
    virtual ~StringColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BinaryColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    BinaryColumnPrinter(std::string&, const Type& type);
    virtual ~BinaryColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class ListColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    ListColumnPrinter(std::string&, const Type& type);
    virtual ~ListColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class MapColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> keyPrinter;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    MapColumnPrinter(std::string&, const Type& type);
    virtual ~MapColumnPrinter() {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class UnionColumnPrinter: public ColumnPrinter {
  private:
    const unsigned char *tags;
    const uint64_t* offsets;
    std::vector<ColumnPrinter*> fieldPrinter;

  public:
    UnionColumnPrinter(std::string&, const Type& type);
    virtual ~UnionColumnPrinter();
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StructColumnPrinter: public ColumnPrinter {
  private:
    std::vector<ColumnPrinter*> fieldPrinter;
  public:
    StructColumnPrinter(std::string&, const Type& type);
    virtual ~StructColumnPrinter();
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  void writeChar(std::string& file, char ch) {
    file += ch;
  }

  void writeString(std::string& file, const char *ptr) {
    size_t len = strlen(ptr);
    file.append(ptr, len);
  }

  ColumnPrinter::ColumnPrinter(std::string& _buffer, const Type& _type
                               ): buffer(_buffer),
                                  type(_type) {
    notNull = nullptr;
    hasNulls = false;
  }

  ColumnPrinter::~ColumnPrinter() {
    // PASS
  }

  void ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    hasNulls = batch.hasNulls;
    if (hasNulls) {
      notNull = batch.notNull.data();
    } else {
      notNull = nullptr ;
    }
  }

  std::unique_ptr<ColumnPrinter> createColumnPrinter(std::string& buffer,
                                                     const Type& type) {
    ColumnPrinter *result;
    switch(static_cast<int64_t>(type.getKind())) {
    case BOOLEAN:
      result = new BooleanColumnPrinter(buffer, type);
      break;

    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      result = new LongColumnPrinter(buffer, type);
      break;

    case FLOAT:
    case DOUBLE:
      result = new DoubleColumnPrinter(buffer, type);
      break;

    case STRING:
    case VARCHAR :
    case CHAR:
      result = new StringColumnPrinter(buffer, type);
      break;

    case BINARY:
      result = new BinaryColumnPrinter(buffer, type);
      break;

    case TIMESTAMP:
      result = new TimestampColumnPrinter(buffer, type);
      break;

    case LIST:
      result = new ListColumnPrinter(buffer, type);
      break;

    case MAP:
      result = new MapColumnPrinter(buffer, type);
      break;

    case STRUCT:
      result = new StructColumnPrinter(buffer, type);
      break;

    case DECIMAL:
      if (type.getPrecision() == 0 || type.getPrecision() > 18) {
        result = new Decimal128ColumnPrinter(buffer, type);
      } else {
        result = new Decimal64ColumnPrinter(buffer, type);
      }
      break;

    case DATE:
      result = new DateColumnPrinter(buffer, type);
      break;

    case UNION:
      result = new UnionColumnPrinter(buffer, type);
      break;

    default:
      throw std::logic_error("unknown batch type");
    }
    return std::unique_ptr<ColumnPrinter>(result);
  }

  LongColumnPrinter::LongColumnPrinter(std::string& buffer,
                                       const Type& type
                                       ): ColumnPrinter(buffer, type) {
    // pass
  }

  void LongColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      char numBuffer[64];
      snprintf(numBuffer, sizeof(numBuffer), "%" INT64_FORMAT_STRING "d",
               static_cast<int64_t >(data[rowId]));
      writeString(buffer, numBuffer);
    }
  }

  DoubleColumnPrinter::DoubleColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer, type),
                                              isFloat(type.getKind() == FLOAT){
    // PASS
  }

  void DoubleColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      char numBuffer[64];
      snprintf(numBuffer, sizeof(numBuffer), isFloat ? "%.7g" : "%.14g",
               data[rowId]);
      writeString(buffer, numBuffer);
    }
  }

  Decimal64ColumnPrinter::Decimal64ColumnPrinter(std::string& buffer,
                                                 const  Type& type
                                                 ): ColumnPrinter(buffer,
                                                                  type) {
    // PASS
  }

  void Decimal64ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const Decimal64VectorBatch&>(batch).values.data();
    scale = dynamic_cast<const Decimal64VectorBatch&>(batch).scale;
  }

  std::string toDecimalString(int64_t value, int32_t scale) {
    std::stringstream buffer;
    if (scale == 0) {
      buffer << value;
      return buffer.str();
    }
    std::string sign = "";
    if (value < 0) {
      sign = "-";
      value = -value;
    }
    buffer << value;
    std::string str = buffer.str();
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
      return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." +
        str.substr(static_cast<size_t>(len - scale),
                   static_cast<size_t>(scale));
    } else if (len == scale) {
      return sign + "0." + str;
    } else {
      std::string result = sign + "0.";
      for(int32_t i=0; i < scale - len; ++i) {
        result += "0";
      }
      return result + str;
    }
  }

  void Decimal64ColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeString(buffer, toDecimalString(data[rowId], scale).c_str());
    }
  }

  Decimal128ColumnPrinter::Decimal128ColumnPrinter(std::string& buffer,
                                                   const Type& type
                                                   ): ColumnPrinter(buffer,
                                                                    type) {
     // PASS
   }

   void Decimal128ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
     ColumnPrinter::reset(batch);
     data = dynamic_cast<const Decimal128VectorBatch&>(batch).values.data();
     scale =dynamic_cast<const Decimal128VectorBatch&>(batch).scale;
   }

   void Decimal128ColumnPrinter::printRow(uint64_t rowId) {
     if (hasNulls && !notNull[rowId]) {
       writeString(buffer, "null");
     } else {
       writeString(buffer, data[rowId].toDecimalString(scale).c_str());
     }
   }

  StringColumnPrinter::StringColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer, type) {
    // PASS
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '"');
      for(int64_t i=0; i < length[rowId]; ++i) {
        char ch = static_cast<char>(start[rowId][i]);
        switch (ch) {
        case '\\':
          writeString(buffer, "\\\\");
          break;
        case '\b':
          writeString(buffer, "\\b");
          break;
        case '\f':
          writeString(buffer, "\\f");
          break;
        case '\n':
          writeString(buffer, "\\n");
          break;
        case '\r':
          writeString(buffer, "\\r");
          break;
        case '\t':
          writeString(buffer, "\\t");
          break;
        case '"':
          writeString(buffer, "\\\"");
          break;
        default:
          writeChar(buffer, ch);
          break;
        }
      }
      writeChar(buffer, '"');
    }
  }

  ListColumnPrinter::ListColumnPrinter(std::string& buffer,
                                       const Type& type
                                       ): ColumnPrinter(buffer, type) {
    elementPrinter = createColumnPrinter(buffer, type.getSubtype(0));
  }

  void ListColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    offsets = dynamic_cast<const ListVectorBatch&>(batch).offsets.data();
    elementPrinter->reset(*dynamic_cast<const ListVectorBatch&>(batch).
                          elements);
  }

  void ListColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          writeString(buffer, ", ");
        }
        elementPrinter->printRow(static_cast<uint64_t>(i));
      }
      writeChar(buffer, ']');
    }
  }

  MapColumnPrinter::MapColumnPrinter(std::string& buffer,
                                     const Type& type
                                     ): ColumnPrinter(buffer, type) {
    keyPrinter = createColumnPrinter(buffer, type.getSubtype(0));
    elementPrinter = createColumnPrinter(buffer, type.getSubtype(1));
  }

  void MapColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const MapVectorBatch& myBatch = dynamic_cast<const MapVectorBatch&>(batch);
    offsets = myBatch.offsets.data();
    keyPrinter->reset(*myBatch.keys);
    elementPrinter->reset(*myBatch.elements);
  }

  void MapColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          writeString(buffer, ", ");
        }
        writeString(buffer, "{\"key\": ");
        keyPrinter->printRow(static_cast<uint64_t>(i));
        writeString(buffer, ", \"value\": ");
        elementPrinter->printRow(static_cast<uint64_t>(i));
        writeChar(buffer, '}');
      }
      writeChar(buffer, ']');
    }
  }

  UnionColumnPrinter::UnionColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer, type) {
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      fieldPrinter.push_back(createColumnPrinter(buffer, type.getSubtype(i))
                             .release());
    }
  }

  UnionColumnPrinter::~UnionColumnPrinter() {
    for (size_t i = 0; i < fieldPrinter.size(); i++) {
      delete fieldPrinter[i];
    }
  }

  void UnionColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const UnionVectorBatch& unionBatch =
      dynamic_cast<const UnionVectorBatch&>(batch);
    tags = unionBatch.tags.data();
    offsets = unionBatch.offsets.data();
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
      fieldPrinter[i]->reset(*(unionBatch.children[i]));
    }
  }

  void UnionColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeString(buffer, "{\"tag\": ");
      char numBuffer[64];
      snprintf(numBuffer, sizeof(numBuffer), "%" INT64_FORMAT_STRING "d",
               static_cast<int64_t>(tags[rowId]));
      writeString(buffer, numBuffer);
      writeString(buffer, ", \"value\": ");
      fieldPrinter[tags[rowId]]->printRow(offsets[rowId]);
      writeChar(buffer, '}');
    }
  }

  StructColumnPrinter::StructColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer, type) {
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      fieldPrinter.push_back(createColumnPrinter(buffer, type.getSubtype(i))
                             .release());
    }
  }

  StructColumnPrinter::~StructColumnPrinter() {
    for (size_t i = 0; i < fieldPrinter.size(); i++) {
      delete fieldPrinter[i];
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
      fieldPrinter[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '{');
      for(unsigned int i=0; i < fieldPrinter.size(); ++i) {
        if (i != 0) {
          writeString(buffer, ", ");
        }
        writeChar(buffer, '"');
        writeString(buffer, type.getFieldName(i).c_str());
        writeString(buffer, "\": ");
        fieldPrinter[i]->printRow(rowId);
      }
      writeChar(buffer, '}');
    }
  }

  DateColumnPrinter::DateColumnPrinter(std::string& buffer,
                                       const Type& type
                                       ): ColumnPrinter(buffer, type) {
    // PASS
  }

  void DateColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      const time_t timeValue = data[rowId] * 24 * 60 * 60;
      struct tm tmValue;
      gmtime_r(&timeValue, &tmValue);
      char timeBuffer[11];
      strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d", &tmValue);
      writeChar(buffer, '"');
      writeString(buffer, timeBuffer);
      writeChar(buffer, '"');
    }
  }

  void DateColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BooleanColumnPrinter::BooleanColumnPrinter(std::string& buffer,
                                             const Type& type
                                             ): ColumnPrinter(buffer, type) {
    // PASS
  }

  void BooleanColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeString(buffer, (data[rowId] ? "true" : "false"));
    }
  }

  void BooleanColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BinaryColumnPrinter::BinaryColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer, type) {
    // PASS
  }

  void BinaryColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for(int64_t i=0; i < length[rowId]; ++i) {
        if (i != 0) {
          writeString(buffer, ", ");
        }
        char numBuffer[64];
        snprintf(numBuffer, sizeof(numBuffer), "%d",
                 (static_cast<const int>(start[rowId][i]) & 0xff));
        writeString(buffer, numBuffer);
      }
      writeChar(buffer, ']');
    }
  }

  void BinaryColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  TimestampColumnPrinter::TimestampColumnPrinter(std::string& buffer,
                                                 const Type& type
                                                 ): ColumnPrinter(buffer,
                                                                  type) {
    struct tm epochTm;
    epochTm.tm_sec = 0;
    epochTm.tm_min = 0;
    epochTm.tm_hour = 0;
    epochTm.tm_mday = 1;
    epochTm.tm_mon = 0;
    epochTm.tm_year = 70;
    epochTm.tm_isdst = 0;
    epoch = mktime(&epochTm);
  }

  void TimestampColumnPrinter::printRow(uint64_t rowId) {
    const int64_t NANOS_PER_SECOND = 1000000000;
    const int64_t NANO_DIGITS = 9;
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      int64_t nanos = data[rowId] % NANOS_PER_SECOND;
      time_t seconds =
        static_cast<time_t>(data[rowId] / NANOS_PER_SECOND) + epoch;
      // make sure the nanos are positive
      if (nanos < 0) {
        seconds -= 1;
        nanos = -nanos;
      }
      struct tm tmValue;
      localtime_r(&seconds, &tmValue);
      char timeBuffer[20];
      strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
      writeChar(buffer, '"');
      writeString(buffer, timeBuffer);
      writeChar(buffer, '.');
      // remove trailing zeros off the back of the nanos value.
      int64_t zeroDigits = 0;
      if (nanos == 0) {
        zeroDigits = 8;
      } else {
        while (nanos % 10 == 0) {
          nanos /= 10;
          zeroDigits += 1;
        }
      }
      char numBuffer[64];
      snprintf(numBuffer, sizeof(numBuffer),
               "%0*" INT64_FORMAT_STRING "d\"",
               static_cast<int>(NANO_DIGITS - zeroDigits),
               static_cast<int64_t >(nanos));
      writeString(buffer, numBuffer);
    }
  }

  void TimestampColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }
}
