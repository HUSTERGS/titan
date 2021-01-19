#include "blob_file_iterator.h"

#include "blob_file_reader.h"
#include "util.h"
#include "util/crc32c.h"

namespace rocksdb
{
    namespace titandb
    {

        BlobFileIterator::BlobFileIterator(
            std::unique_ptr<RandomAccessFileReader> &&file, uint64_t file_name,
            uint64_t file_size, const TitanCFOptions &titan_cf_options)
            : file_(std::move(file)),
              file_number_(file_name),
              file_size_(file_size),
              titan_cf_options_(titan_cf_options) {}

        BlobFileIterator::~BlobFileIterator() {}

        // 整个过程主要是为了正确的计算几个字段，包括`header_size_`，`end_of_blob_record_`
        bool BlobFileIterator::Init()
        {
            Slice slice;
            char header_buf[BlobFileHeader::kMaxEncodedLength];
            // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
            // is only used for GC, we always set for_compaction to true.
            // 如果for_compaction被设置为true，那么rate_limiter就会被启用，而BlobFileIterator只被用于
            // GC，所以所以调用read的时候使用将for_compaction字段设置为true
            // 由于Blob文件的头部有两种版本，所以先读取kMaxEncodeLength，可以保证对两种格式都妥善处理
            status_ = file_->Read(0, BlobFileHeader::kMaxEncodedLength, &slice,
                                  header_buf, true /*for_compaction*/);
            if (!status_.ok())
            {
                return false;
            }
            BlobFileHeader blob_file_header;

            status_ = blob_file_header.DecodeFrom(&slice);
            if (!status_.ok())
            {
                return false;
            }

            header_size_ = blob_file_header.size();

            char footer_buf[BlobFileFooter::kEncodedLength];
            // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
            // is only used for GC, we always set for_compaction to true.
            status_ = file_->Read(file_size_ - BlobFileFooter::kEncodedLength,
                                  BlobFileFooter::kEncodedLength, &slice, footer_buf,
                                  true /*for_compaction*/);
            if (!status_.ok())
                return false;
            BlobFileFooter blob_file_footer;
            status_ = blob_file_footer.DecodeFrom(&slice);
            // blob_record的末尾位置就是文件大小减去footer的长度，不知道有没有考虑meta
            end_of_blob_record_ = file_size_ - BlobFileFooter::kEncodedLength;
            // 考虑了，如果meta_index_handle不是null的话，那么就还要继续减少end_of_blob_record_
            if (!blob_file_footer.meta_index_handle.IsNull())
            {
                end_of_blob_record_ -=
                    (blob_file_footer.meta_index_handle.size() + kBlockTrailerSize);
            }
            // 以上分别获取了blob文件的文件头以及文件footer
            if (blob_file_header.flags & BlobFileHeader::kHasUncompressionDictionary)
            {
                status_ = InitUncompressionDict(blob_file_footer, file_.get(),
                                                &uncompression_dict_);
                if (!status_.ok())
                {
                    return false;
                }
                decoder_.SetUncompressionDict(uncompression_dict_.get());
                // the layout of blob file is like:
                // |  ....   |
                // | records |
                // | compression dict + kBlockTrailerSize(5) |
                // | metaindex block(40) + kBlockTrailerSize(5) |
                // | footer(kEncodedLength: 32) |
                end_of_blob_record_ -=
                    (uncompression_dict_->GetRawDict().size() + kBlockTrailerSize);
            }

            assert(end_of_blob_record_ > BlobFileHeader::kMinEncodedLength);
            init_ = true;
            return true;
        }
        // 跳转到最开始的记录，也就是将iterate_offset_设置为header_size_
        void BlobFileIterator::SeekToFirst()
        {
            if (!init_ && !Init())
                return;
            status_ = Status::OK();
            iterate_offset_ = header_size_;
            PrefetchAndGet();
        }

        bool BlobFileIterator::Valid() const { return valid_ && status().ok(); }

        void BlobFileIterator::Next()
        {
            assert(init_);
            PrefetchAndGet();
        }

        Slice BlobFileIterator::key() const { return cur_blob_record_.key; }

        Slice BlobFileIterator::value() const { return cur_blob_record_.value; }

        /**
         * 跳转到从offset开始的第一个完整的record
         * 因为blob record是紧密排放的，并且长度不固定，所以没有一步到位的办法，只有一个一个读取record
         * 然后与offset进行比较来找到对应的record
         * @param offset
         */
        void BlobFileIterator::IterateForPrev(uint64_t offset)
        {
            // 如果没有init，那么就进行init
            // 如果init失败就会直接返回
            if (!init_ && !Init())
                return;

            status_ = Status::OK();
            // 如果设定的offset超出了end_of_blob_record_
            // 依然会进行设置，但是会直接返回
            if (offset >= end_of_blob_record_)
            {
                iterate_offset_ = offset;
                status_ = Status::InvalidArgument("Out of bound");
                return;
            }

            uint64_t total_length = 0;
            // 用于单个record header的buffer
            FixedSlice<kRecordHeaderSize> header_buffer;
            iterate_offset_ = header_size_;

            for (; iterate_offset_ < offset; iterate_offset_ += total_length)
            {
                // With for_compaction=true, rate_limiter is enabled. Since
                // BlobFileIterator is only used for GC, we always set for_compaction to
                // true.
                status_ = file_->Read(iterate_offset_, kRecordHeaderSize, &header_buffer,
                                      header_buffer.get(), true /*for_compaction*/);
                if (!status_.ok())
                    return;
                status_ = decoder_.DecodeHeader(&header_buffer);
                if (!status_.ok())
                    return;
                total_length = kRecordHeaderSize + decoder_.GetRecordSize();
            }
            // 如果超出了就退回到前一个
            // 但是实际上想要退出上面的循环的话，iterate_offset_必然是大于offset的
            if (iterate_offset_ > offset)
                iterate_offset_ -= total_length;
            valid_ = false;
        }

        // 获取当前offset(也即是iterate_offset_)对应的blob record，并将结果保存在cur_blob_record_当中
        void BlobFileIterator::GetBlobRecord()
        {
            FixedSlice<kRecordHeaderSize> header_buffer;
            // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
            // is only used for GC, we always set for_compaction to true.
            status_ = file_->Read(iterate_offset_, kRecordHeaderSize, &header_buffer,
                                  header_buffer.get(), true /*for_compaction*/);
            if (!status_.ok())
                return;
            status_ = decoder_.DecodeHeader(&header_buffer);
            if (!status_.ok())
                return;

            Slice record_slice;
            auto record_size = decoder_.GetRecordSize();
            buffer_.resize(record_size);
            // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
            // is only used for GC, we always set for_compaction to true.
            status_ = file_->Read(iterate_offset_ + kRecordHeaderSize, record_size,
                                  &record_slice, buffer_.data(), true /*for_compaction*/);
            if (status_.ok())
            {
                status_ =
                    decoder_.DecodeRecord(&record_slice, &cur_blob_record_, &uncompressed_);
            }
            if (!status_.ok())
                return;

            cur_record_offset_ = iterate_offset_;
            cur_record_size_ = kRecordHeaderSize + record_size;
            iterate_offset_ += cur_record_size_;
            valid_ = true;
        }

        // 大概就是不断调整readahead_begin_offset以及readahead_end_offset以及readahead_size来决定预读取的数据，
        // 然后调用GetBlobRecord来获得当前的blob记录
        // 但是并没有感觉到有实际的`prefetch`？
        void BlobFileIterator::PrefetchAndGet()
        {
            if (iterate_offset_ >= end_of_blob_record_)
            {
                valid_ = false;
                return;
            }
            // 如果当前的iterate_offset_的位置已经不在readahead的区间当中的话，就需要对其进行调整
            if (readahead_begin_offset_ > iterate_offset_ ||
                readahead_end_offset_ < iterate_offset_)
            {
                // alignment
                readahead_begin_offset_ =
                    iterate_offset_ - (iterate_offset_ & (kDefaultPageSize - 1));
                readahead_end_offset_ = readahead_begin_offset_;
                readahead_size_ = kMinReadaheadSize;
            }
            auto min_blob_size =
                iterate_offset_ + kRecordHeaderSize + titan_cf_options_.min_blob_size;
            if (readahead_end_offset_ <= min_blob_size)
            {
                while (readahead_end_offset_ + readahead_size_ <= min_blob_size &&
                       readahead_size_ < kMaxReadaheadSize)
                    readahead_size_ <<= 1;
                file_->Prefetch(readahead_end_offset_, readahead_size_);
                readahead_end_offset_ += readahead_size_;
                readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ << 1);
            }

            GetBlobRecord();

            if (readahead_end_offset_ < iterate_offset_)
            {
                readahead_end_offset_ = iterate_offset_;
            }
        }

        BlobFileMergeIterator::BlobFileMergeIterator(
            std::vector<std::unique_ptr<BlobFileIterator>> &&blob_file_iterators,
            const Comparator *comparator)
            : blob_file_iterators_(std::move(blob_file_iterators)),
              min_heap_(BlobFileIterComparator(comparator)) {}

        bool BlobFileMergeIterator::Valid() const
        {
            if (current_ == nullptr)
                return false;
            if (!status().ok())
                return false;
            return current_->Valid() && current_->status().ok();
        }


        void BlobFileMergeIterator::SeekToFirst()
        {
            for (auto &iter : blob_file_iterators_)
            {
                iter->SeekToFirst();
                if (iter->status().ok() && iter->Valid())
                    min_heap_.push(iter.get());
            }
            if (!min_heap_.empty())
            {
                current_ = min_heap_.top();
                min_heap_.pop();
            }
            else
            {
                status_ = Status::Aborted("No iterator is valid");
            }
        }
        // 从current_也就是当前的blob文件的iterator中获得key。
        // 因为这一个iterator发生了改变，所以需要将其重新加入堆中进行排序
        // 同时取出原来的current_
        void BlobFileMergeIterator::Next()
        {
            assert(Valid());
            current_->Next();

            if (current_->status().ok() && current_->Valid())
                min_heap_.push(current_);
            if (!min_heap_.empty())
            {
                current_ = min_heap_.top();
                min_heap_.pop();
            }
            else
            {
                current_ = nullptr;
            }
        }

        Slice BlobFileMergeIterator::key() const
        {
            assert(current_ != nullptr);
            return current_->key();
        }

        Slice BlobFileMergeIterator::value() const
        {
            assert(current_ != nullptr);
            return current_->value();
        }

    } // namespace titandb
} // namespace rocksdb
