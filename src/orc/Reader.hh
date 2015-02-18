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

#ifndef ORC_READER_HH
#define ORC_READER_HH

#include "Vector.hh"

#include <memory>
#include <string>
#include <vector>

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  struct ReaderOptionsPrivate;

  enum CompressionKind {
    CompressionKind_NONE = 0,
    CompressionKind_ZLIB = 1,
    CompressionKind_SNAPPY = 2,
    CompressionKind_LZO = 3
  };

  /**
   * Statistics that are available for all types of columns.
   */
  class ColumnStatistics {
  public:
    virtual ~ColumnStatistics();

    /**
     * Get the number of values in this column. It will differ from the number
     * of rows because of NULL values and repeated values.
     * @return the number of values
     */
    virtual uint64_t getNumberOfValues() const = 0;

    /**
     * print out statistics of column if any
     */
    virtual std::string toString() const = 0;
  };

  /**
   * Statistics for binary columns.
   */
  class BinaryColumnStatistics: public ColumnStatistics {
  public:
    virtual ~BinaryColumnStatistics();

    virtual uint64_t getTotalLength() const = 0;
  };

  /**
   * Statistics for boolean columns.
   */
  class BooleanColumnStatistics: public ColumnStatistics {
  public:
    virtual ~BooleanColumnStatistics();

    virtual uint64_t getFalseCount() const = 0;
    virtual uint64_t getTrueCount() const = 0;
  };

  /**
   * Statistics for date columns.
   */
  class DateColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DateColumnStatistics();

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual int32_t getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int32_t getMaximum() const = 0;
  };

  /**
   * Statistics for decimal columns.
   */
  class DecimalColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DecimalColumnStatistics();

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual Decimal getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual Decimal getMaximum() const = 0;

    /**
     * Get the sum for the column.
     * @return sum of all the values
     */
    virtual Decimal getSum() const = 0;
  };

  /**
   * Statistics for float and double columns.
   */
  class DoubleColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DoubleColumnStatistics();

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual double getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual double getMaximum() const = 0;

    /**
     * Get the sum of the values in the column.
     * @return the sum
     */
    virtual double getSum() const = 0;
  };

  /**
   * Statistics for all of the integer columns, such as byte, short, int, and
   * long.
   */
  class IntegerColumnStatistics: public ColumnStatistics {
  public:
    virtual ~IntegerColumnStatistics();

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * Is the sum defined? If the sum overflowed the counter this will be
     * false.
     * @return is the sum available
     */
    virtual bool isSumDefined() const = 0;

    /**
     * Get the sum of the column. Only valid if isSumDefined returns true.
     * @return the sum of the column
     */
    virtual int64_t getSum() const = 0;
  };

  /**
   * Statistics for string columns.
   */
  class StringColumnStatistics: public ColumnStatistics {
  public:
    virtual ~StringColumnStatistics();

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual std::string getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual std::string getMaximum() const = 0;

    /**
     * Get the total length of all values.
     * @return total length of all the values
     */
    virtual uint64_t getTotalLength() const = 0;
  };

  /**
   * Statistics for timestamp columns.
   */
  class TimestampColumnStatistics: public ColumnStatistics {
  public:
    virtual ~TimestampColumnStatistics();

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int64_t getMaximum() const = 0;
  };

  class StripeInformation {
  public:
    virtual ~StripeInformation();

    /**
     * Get the byte offset of the start of the stripe.
     * @return the bytes from the start of the file
     */
    virtual unsigned long getOffset() const = 0;

    /**
     * Get the total length of the stripe in bytes.
     * @return the number of bytes in the stripe
     */
    virtual unsigned long getLength() const = 0;

    /**
     * Get the length of the stripe's indexes.
     * @return the number of bytes in the index
     */
    virtual unsigned long getIndexLength() const = 0;

    /**
     * Get the length of the stripe's data.
     * @return the number of bytes in the stripe
     */
    virtual unsigned long getDataLength()const = 0;

    /**
     * Get the length of the stripe's tail section, which contains its index.
     * @return the number of bytes in the tail
     */
    virtual unsigned long getFooterLength() const = 0;

    /**
     * Get the number of rows in the stripe.
     * @return a count of the number of rows
     */
    virtual unsigned long getNumberOfRows() const = 0;
  };

  class StripeStatistics {
  public:
    virtual ~StripeStatistics();

    /**
     * Get the statistics of indexth col in the stripe.
     * @return one column's statistics
     */
    virtual std::unique_ptr<ColumnStatistics>
    getColumnStatisticsInStripe(unsigned long index) const = 0;

    /**
     * Get the statistics of all cols in the stripe.
     * @return all columns' statistics
     */
    virtual std::list<ColumnStatistics*> getStatisticsInStripe() const = 0;

    /**
     * Get the number of columns in this stripe
     * @return columnstatistics
     */
    virtual unsigned long getNumberOfColumnStatistics() const = 0;
  };


  /**
   * Options for creating a Reader.
   */
  class ReaderOptions {
  private:
    std::unique_ptr<ReaderOptionsPrivate> privateBits;

  public:
    ReaderOptions();
    ReaderOptions(const ReaderOptions&);
    ReaderOptions(ReaderOptions&);
    ReaderOptions& operator=(const ReaderOptions&);
    virtual ~ReaderOptions();

    /**
     * Set the list of columns to read. All columns that are children of
     * selected columns are automatically selected. The default value is
     * {0}.
     * @param include a list of columns to read
     * @return this
     */
    ReaderOptions& include(const std::list<int>& include);

    /**
     * Set the list of columns to read. All columns that are children of
     * selected columns are automatically selected. The default value is
     * {0}.
     * @param include a list of columns to read
     * @return this
     */
    ReaderOptions& include(std::vector<int> include);

    /**
     * Set the section of the file to process.
     * @param offset the starting byte offset
     * @param length the number of bytes to read
     * @return this
     */
    ReaderOptions& range(unsigned long offset, unsigned long length);

    /**
     * For Hive 0.11 (and 0.12) decimals, the precision was unlimited
     * and thus may overflow the 38 digits that is supported. If one
     * of the Hive 0.11 decimals is too large, the reader may either convert
     * the value to NULL or throw an exception. That choice is controlled
     * by this setting.
     *
     * Defaults to true.
     *
     * @param shouldThrow should the reader throw a ParseError?
     * @return returns *this
     */
    ReaderOptions& throwOnHive11DecimalOverflow(bool shouldThrow);

    /**
     * For Hive 0.11 (and 0.12) written decimals, which have unlimited
     * scale and precision, the reader forces the scale to a consistent
     * number that is configured. This setting changes the scale that is
     * forced upon these old decimals. See also throwOnHive11DecimalOverflow.
     *
     * Defaults to 6.
     *
     * @param forcedScale the scale that will be forced on Hive 0.11 decimals
     * @return returns *this
     */
    ReaderOptions& forcedScaleOnHive11Decimal(int32_t forcedScale);

    /**
     * Set the location of the tail as defined by the logical length of the
     * file.
     */
    ReaderOptions& setTailLocation(unsigned long offset);

    /**
     * Set the stream to use for printing warning or error messages.
     */
    ReaderOptions& setErrorStream(std::ostream& stream);

    /**
     * Get the list of selected columns to read. All children of the selected
     * columns are also selected.
     */
    const std::list<int>& getInclude() const;

    /**
     * Get the start of the range for the data being processed.
     * @return if not set, return 0
     */
    unsigned long getOffset() const;

    /**
     * Get the end of the range for the data being processed.
     * @return if not set, return the maximum long
     */
    unsigned long getLength() const;

    /**
     * Get the desired tail location.
     * @return if not set, return the maximum long.
     */
    unsigned long getTailLocation() const;

    /**
     * Should the reader throw a ParseError when a Hive 0.11 decimal is
     * larger than the supported 38 digits of precision? Otherwise, the
     * data item is replaced by a NULL.
     */
    bool getThrowOnHive11DecimalOverflow() const;

    /**
     * What scale should all Hive 0.11 decimals be normalized to?
     */
    int32_t getForcedScaleOnHive11Decimal() const;

    /**
     * Get the stream to write warnings or errors to.
     */
    std::ostream* getErrorStream() const;
  };

  /**
   * The interface for reading ORC files.
   * This is an an abstract class that will subclassed as necessary.
   */
  class Reader {
  public:
    virtual ~Reader();

    /**
     * Get the number of rows in the file.
     * @return the number of rows
     */
    virtual unsigned long getNumberOfRows() const = 0;

    /**
     * Get the user metadata keys.
     * @return the set of metadata keys
     */
    virtual std::list<std::string> getMetadataKeys() const = 0;

    /**
     * Get a user metadata value.
     * @param key a key given by the user
     * @return the bytes associated with the given key
     */
    virtual std::string getMetadataValue(const std::string& key) const = 0;

    /**
     * Did the user set the given metadata value.
     * @param key the key to check
     * @return true if the metadata value was set
     */
    virtual bool hasMetadataValue(const std::string& key) const = 0;

    /**
     * Get the compression kind.
     * @return the kind of compression in the file
     */
    virtual CompressionKind getCompression() const = 0;

    /**
     * Get the buffer size for the compression.
     * @return number of bytes to buffer for the compression codec.
     */
    virtual unsigned long getCompressionSize() const = 0;

    /**
     * Get the number of rows per a entry in the row index.
     * @return the number of rows per an entry in the row index or 0 if there
     * is no row index.
     */
    virtual unsigned long getRowIndexStride() const = 0;

    /**
     * Get the number of stripes in the file.
     * @return the number of stripes
     */
    virtual unsigned long getNumberOfStripes() const = 0;

    /**
     * Get the information about a stripe.
     * @param stripeIndex the stripe 0 to N-1 to get information about
     * @return the information about that stripe
     */
    virtual std::unique_ptr<StripeInformation>
    getStripe(unsigned long stripeIndex) const = 0;

    /**
     * Get the statistics about a stripe.
     * @param stripeIndex the stripe 0 to N-1 to get statistics about
     * @return the statistics about that stripe
     */
    virtual std::unique_ptr<StripeStatistics>
    getStripeStatistics(unsigned long stripeIndex) const = 0;

    /**
     * Get the length of the file.
     * @return the number of bytes in the file
     */
    virtual unsigned long getContentLength() const = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual std::list<ColumnStatistics*> getStatistics() const = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual std::unique_ptr<ColumnStatistics>
    getColumnStatistics(unsigned long index) const = 0;

    /**
     * Get the type of the rows in the file. The top level is always a struct.
     * @return the root type
     */
    virtual const Type& getType() const = 0;

    /**
     * Get the selected columns of the file.
     */
    virtual const std::vector<bool> getSelectedColumns() const = 0;

    /**
     * Create a row batch for reading the selected columns of this file.
     * @param size the number of rows to read
     * @return a new ColumnVectorBatch to read into
     */
    virtual std::unique_ptr<ColumnVectorBatch> createRowBatch
    (unsigned long size) const = 0;

    /**
     * Read the next row batch from the current position.
     * Caller must look at numElements in the row batch to determine how
     * many rows were read.
     * @param data the row batch to read into.
     * @return true if a non-zero number of rows were read or false if the
     *   end of the file was reached.
     */
    virtual bool next(ColumnVectorBatch& data) = 0;

    /**
     * Get the row number of the first row in the previously read batch.
     * @return the row number of the previous batch.
     */
    virtual unsigned long getRowNumber() const = 0;

    /**
     * Seek to a given row.
     * @param rowNumber the next row the reader should return
     */
    virtual void seekToRow(unsigned long rowNumber) = 0;

    /**
     * Get the name of the input stream.
     */
    virtual const std::string& getStreamName() const = 0;
  };
}

#endif
