#include "blob_format.h"

#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace rocksdb
{
    namespace titandb
    {
        // 不是很懂这一层namespace是什么意思
        namespace
        {
            // 从Slice中获得第一个char，同时将Slice中的第一个char去除，类似于pop操作
            bool GetChar(Slice *src, unsigned char *value)
            {
                if (src->size() < 1)
                    return false;
                *value = *src->data();
                src->remove_prefix(1);
                return true;
            }

        } // namespace

        /******************************* 以下是BlobRecord相关成员函数 ******************************/

        /**
         * 将key和value保存在dst中，其中包含了将其提取出来的所有信息（长度）
         * @param dst 目标位置
         */
        void BlobRecord::EncodeTo(std::string *dst) const
        {
            PutLengthPrefixedSlice(dst, key);
            PutLengthPrefixedSlice(dst, value);
        }
        /**
         * 从src中将已经encode的信息，包括键和值重新取出来
         * @param src 数据源
         * @return 是否成功
         * @details 调用这个方法的时候应该是使用一个空的BlobRecord，解析出来的数据（key/value）会直接存放在对象本身的成员中
         */
        Status BlobRecord::DecodeFrom(Slice *src)
        {
            if (!GetLengthPrefixedSlice(src, &key) ||
                !GetLengthPrefixedSlice(src, &value))
            {
                return Status::Corruption("BlobRecord");
            }
            return Status::OK();
        }
        // 相等判断
        bool operator==(const BlobRecord &lhs, const BlobRecord &rhs)
        {
            return lhs.key == rhs.key && lhs.value == rhs.value;
        }

        /******************************* 以下是关于BlobEncoder的成员函数 ****************************
         *          主要负责加上专属于每一个record的头部信息，包括crc，大小以及压缩模式
         ************************************************************************************
         */

        /**
         * 将record转化为Slice之后进行压缩
         * @param record 需要进行encode压缩的数据
         * @details
         * 一开始record是一个结构体，需要将其保存为Slice，具体存放在record_buffer_中
         * 然后需要将Slice再进行压缩/crc等操作，保存在record_成员中
         */
        void BlobEncoder::EncodeRecord(const BlobRecord &record)
        {
            record_buffer_.clear();
            record.EncodeTo(&record_buffer_);
            EncodeSlice(record_buffer_);
        }
        /**
         * 主要负责将一个Slice(Record进行转化之后的Slice)进行压缩并加上头部信息
         * @param record 要进行存储（可能发生压缩）的记录
         * @details
         * 1. 首先清除compressed_buffer_，为之后可能发生的压缩提供空间
         * 2. 将record进行压缩，保存在record_成员中
         * 3. 分别填充header的
         *  3.1 size (record_的长度，也就是压缩好的数据的长度)
         *  3.2 compression 压缩类型
         *  3.3 crc校验值
         *
         *  最终有用的数据保存在record_以及header_成员中
         */
        void BlobEncoder::EncodeSlice(const Slice &record)
        {
            compressed_buffer_.clear();
            CompressionType compression;
            record_ =
                Compress(*compression_info_, record, &compressed_buffer_, &compression);

            assert(record_.size() < std::numeric_limits<uint32_t>::max());
            // header的存储顺序是[crc][4] + [size][4] + [compression][1]，所以从第四个字节开始存储record的大小
            EncodeFixed32(header_ + 4, static_cast<uint32_t>(record_.size()));
            // 存储压缩类型
            header_[8] = compression;
            // 先计算长度和压缩类型的crc
            uint32_t crc = crc32c::Value(header_ + 4, sizeof(header_) - 4);
            // 然后再将其扩充到数据本身
            crc = crc32c::Extend(crc, record_.data(), record_.size());
            // 保存在header的crc位置上
            EncodeFixed32(header_, crc);
        }
        
        /************** 以下是BlobDecoder的成员函数 ******************/

        /**
         * 将src中的信息保存到BlobDecoder当中，用于后续解码，主要就是头部的三个信息，crc，size以及压缩模式
         * @param src 数据源
         * @return 是否成功
         */
        Status BlobDecoder::DecodeHeader(Slice *src)
        {
            if (!GetFixed32(src, &crc_))
            {
                return Status::Corruption("BlobHeader");
            }
            header_crc_ = crc32c::Value(src->data(), kRecordHeaderSize - 4);

            unsigned char compression;
            if (!GetFixed32(src, &record_size_) || !GetChar(src, &compression))
            {
                return Status::Corruption("BlobHeader");
            }

            compression_ = static_cast<CompressionType>(compression);
            return Status::OK();
        }
        /**
         * 将Record从src中解析出来，并保存在参数record当中，其中可能会进行解压缩操作
         * @param src 数据源
         * @param record 用于存放解析出来的数据
         * @param buffer 不懂，似乎是用来临时保存解压出来的数据
         * @return 是否成功
         *
         */
        Status BlobDecoder::DecodeRecord(Slice *src, BlobRecord *record,
                                         OwnedSlice *buffer)
        {

            // QUES: 这是什么？
            TEST_SYNC_POINT_CALLBACK("BlobDecoder::DecodeRecord", &crc_);
            // 对数据做一个备份，因为下面要remove_prefix？
            Slice input(src->data(), record_size_);
            // QUES: 为何会有这个操作，主要是不知道调用的时候src是个什么状态
            // 可以发现后面其实都没有用到src，大概是因为src可能包含了很多歌record，这一步操作相当于是将src指向下一条数据
            src->remove_prefix(record_size_);
            uint32_t crc = crc32c::Extend(header_crc_, input.data(), input.size());
            if (crc != crc_)
            {
                return Status::Corruption("BlobRecord", "checksum mismatch");
            }
            // 如果没有进行压缩就直接decode
            if (compression_ == kNoCompression)
            {
                return DecodeInto(input, record);
            }
            UncompressionContext ctx(compression_);
            UncompressionInfo info(ctx, *uncompression_dict_, compression_);
            Status s = Uncompress(info, input, buffer);
            if (!s.ok())
            {
                return s;
            }
            return DecodeInto(*buffer, record);
        }
        /*************** 以下是关于BlobHandle的成员函数 *************/
        /**
         * 字面意思
         * @param dst
         */
        void BlobHandle::EncodeTo(std::string *dst) const
        {
            PutVarint64(dst, offset);
            PutVarint64(dst, size);
        }
        /**
         * 字面意思
         * @param src
         * @return
         */
        Status BlobHandle::DecodeFrom(Slice *src)
        {
            if (!GetVarint64(src, &offset) || !GetVarint64(src, &size))
            {
                return Status::Corruption("BlobHandle");
            }
            return Status::OK();
        }

        bool operator==(const BlobHandle &lhs, const BlobHandle &rhs)
        {
            return lhs.offset == rhs.offset && lhs.size == rhs.size;
        }


        /****************** 以下是关于BlobIndex的成员函数 ******************/

        // 实际上这种encode/decode基本上都是把成员一个个编码/解码到Slice/string中？
        void BlobIndex::EncodeTo(std::string *dst) const
        {
            dst->push_back(kBlobRecord);
            PutVarint64(dst, file_number);
            blob_handle.EncodeTo(dst);
        }

        Status BlobIndex::DecodeFrom(Slice *src)
        {
            unsigned char type;
            if (!GetChar(src, &type) || type != kBlobRecord ||
                !GetVarint64(src, &file_number))
            {
                return Status::Corruption("BlobIndex");
            }
            Status s = blob_handle.DecodeFrom(src);
            if (!s.ok())
            {
                return Status::Corruption("BlobIndex", s.ToString());
            }
            return s;
        }

        /**
         * deletion marker对应的BlobHandle就是一个空的？而且BlobIndex对应的file_number为0
         * @param dst 目标位置
         * @details
         * 创建一个deletion marker，依次存放`类型`，`文件编号`，`BlobHandle`
         */
        void BlobIndex::EncodeDeletionMarkerTo(std::string *dst)
        {
            dst->push_back(kBlobRecord);
            PutVarint64(dst, 0);
            BlobHandle dummy;
            dummy.EncodeTo(dst);
        }

        bool BlobIndex::IsDeletionMarker(const BlobIndex &index)
        {
            return index.file_number == 0;
        }

        bool BlobIndex::operator==(const BlobIndex &rhs) const
        {
            return (file_number == rhs.file_number && blob_handle == rhs.blob_handle);
        }
        /************************* 以下是MergBlobIndex的相关成员函数 *************************/

        // 将MergBlobIndex编码到dst中，也就是分别调用基类BlobIndex的EncodeTo方法
        // 然后保存自己的新成员source_file_number以及source_file_offset
        void MergeBlobIndex::EncodeTo(std::string *dst) const
        {
            BlobIndex::EncodeTo(dst);
            PutVarint64(dst, source_file_number);
            PutVarint64(dst, source_file_offset);
        }
        // 调用基类BlobIndex的Encode方法
        void MergeBlobIndex::EncodeToBase(std::string *dst) const
        {
            BlobIndex::EncodeTo(dst);
        }

        Status MergeBlobIndex::DecodeFrom(Slice *src)
        {
            Status s = BlobIndex::DecodeFrom(src);
            if (!s.ok())
            {
                return s;
            }
            if (!GetVarint64(src, &source_file_number) ||
                !GetVarint64(src, &source_file_offset))
            {
                return Status::Corruption("MergeBlobIndex");
            }
            return s;
        }

        Status MergeBlobIndex::DecodeFromBase(Slice *src)
        {
            return BlobIndex::DecodeFrom(src);
        }

        bool MergeBlobIndex::operator==(const MergeBlobIndex &rhs) const
        {
            return (source_file_number == rhs.source_file_number &&
                    source_file_offset == rhs.source_file_offset &&
                    BlobIndex::operator==(rhs));
        }

        /****************** 以下是关于BlobFileMeta的相关成员函数 ******************/
        /**
         * 直接存储各个成员，其中Slice类型的smallest_key以及largest_key需要先存放长度
         * @param dst
         */
        void BlobFileMeta::EncodeTo(std::string *dst) const
        {
            PutVarint64(dst, file_number_);
            PutVarint64(dst, file_size_);
            PutVarint64(dst, file_entries_);
            PutVarint32(dst, file_level_);
            PutLengthPrefixedSlice(dst, smallest_key_);
            PutLengthPrefixedSlice(dst, largest_key_);
        }
        /**
         * 针对过去版本的decode函数，简单的取出file_number以及file_size即可
         * @param src
         * @return
         */
        Status BlobFileMeta::DecodeFromLegacy(Slice *src)
        {
            if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_))
            {
                return Status::Corruption("BlobFileMeta decode legacy failed");
            }
            assert(smallest_key_.empty());
            assert(largest_key_.empty());
            return Status::OK();
        }
        /**
         * decode，分别取出各个成员
         * @param src
         * @return
         */
        Status BlobFileMeta::DecodeFrom(Slice *src)
        {
            if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_) ||
                !GetVarint64(src, &file_entries_) || !GetVarint32(src, &file_level_))
            {
                return Status::Corruption("BlobFileMeta decode failed");
            }
            Slice str;
            if (GetLengthPrefixedSlice(src, &str))
            {
                smallest_key_.assign(str.data(), str.size());
            }
            else
            {
                return Status::Corruption("BlobSmallestKey Decode failed");
            }
            if (GetLengthPrefixedSlice(src, &str))
            {
                largest_key_.assign(str.data(), str.size());
            }
            else
            {
                return Status::Corruption("BlobLargestKey decode failed");
            }
            return Status::OK();
        }

        bool operator==(const BlobFileMeta &lhs, const BlobFileMeta &rhs)
        {
            return (lhs.file_number_ == rhs.file_number_ &&
                    lhs.file_size_ == rhs.file_size_ &&
                    lhs.file_entries_ == rhs.file_entries_ &&
                    lhs.file_level_ == rhs.file_level_);
        }
        /**
         * 根据事件的类型将文件的状态转换为指定状态
         * @param event 指定的事件类型
         */
        void BlobFileMeta::FileStateTransit(const FileEvent &event)
        {
            switch (event)
            {
            case FileEvent::kFlushCompleted:
                // blob file maybe generated by flush or gc, because gc will rewrite valid
                // keys to memtable. If it's generated by gc, we will leave gc to change
                // its file state. If it's generated by flush, we need to change it to
                // normal state after flush completed.
                assert(state_ == FileState::kPendingLSM ||
                       state_ == FileState::kPendingGC || state_ == FileState::kNormal ||
                       state_ == FileState::kBeingGC || state_ == FileState::kObsolete);
                if (state_ == FileState::kPendingLSM)
                    state_ = FileState::kNormal;
                break;
            case FileEvent::kGCCompleted:
                // file is marked obsoleted during gc
                if (state_ == FileState::kObsolete)
                {
                    break;
                }
                assert(state_ == FileState::kPendingGC || state_ == FileState::kBeingGC);
                state_ = FileState::kNormal;
                break;
            case FileEvent::kCompactionCompleted:
                assert(state_ == FileState::kPendingLSM);
                state_ = FileState::kNormal;
                break;
            case FileEvent::kGCBegin:
                assert(state_ == FileState::kNormal);
                state_ = FileState::kBeingGC;
                break;
            case FileEvent::kGCOutput:
                assert(state_ == FileState::kInit);
                state_ = FileState::kPendingGC;
                break;
            case FileEvent::kFlushOrCompactionOutput:
                assert(state_ == FileState::kInit);
                state_ = FileState::kPendingLSM;
                break;
            case FileEvent::kDbRestart:
                assert(state_ == FileState::kInit);
                state_ = FileState::kNormal;
                break;
            case FileEvent::kDelete:
                assert(state_ != FileState::kObsolete);
                state_ = FileState::kObsolete;
                break;
            case FileEvent::kNeedMerge:
                if (state_ == FileState::kToMerge)
                {
                    break;
                }
                assert(state_ == FileState::kNormal);
                state_ = FileState::kToMerge;
                break;
            case FileEvent::kReset:
                state_ = FileState::kNormal;
                break;
            default:
                assert(false);
            }
        }
        /**
         * 根据某一个blob文件的ratio的具体`数值`返回一个标定Level的值
         * @return
         */
        TitanInternalStats::StatsType BlobFileMeta::GetDiscardableRatioLevel() const
        {
            auto ratio = GetDiscardableRatio();
            TitanInternalStats::StatsType type;
            if (ratio < std::numeric_limits<double>::epsilon())
            {
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE0;
            }
            else if (ratio <= 0.2)
            {
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE20;
            }
            else if (ratio <= 0.5)
            {
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE50;
            }
            else if (ratio <= 0.8)
            {
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE80;
            }
            else if (ratio <= 1.0 ||
                     (ratio - 1.0) < std::numeric_limits<double>::epsilon())
            {
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100;
            }
            else
            {
                fprintf(stderr, "invalid discardable ratio  %lf for blob file %" PRIu64,
                        ratio, this->file_number_);
                type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100;
            }
            return type;
        }
        /****************** 以下是关于BlobFileHeader的相关成员函数 ******************/
        /**
         * 依次存放三个成员
         * @param dst
         */
        void BlobFileHeader::EncodeTo(std::string *dst) const
        {
            PutFixed32(dst, kHeaderMagicNumber);
            PutFixed32(dst, version);

            if (version == BlobFileHeader::kVersion2)
            {
                PutFixed32(dst, flags);
            }
        }
        /**
         * decode函数可以处理两种不同version的头部格式
         * @param src
         * @return
         */
        Status BlobFileHeader::DecodeFrom(Slice *src)
        {
            uint32_t magic_number = 0;
            if (!GetFixed32(src, &magic_number) || magic_number != kHeaderMagicNumber)
            {
                return Status::Corruption(
                    "Blob file header magic number missing or mismatched.");
            }
            if (!GetFixed32(src, &version) ||
                (version != kVersion1 && version != kVersion2))
            {
                return Status::Corruption("Blob file header version missing or invalid.");
            }
            if (version == BlobFileHeader::kVersion2)
            {
                // Check that no other flags are set
                if (!GetFixed32(src, &flags) || flags & ~kHasUncompressionDictionary)
                {
                    return Status::Corruption("Blob file header flags missing or invalid.");
                }
            }
            return Status::OK();
        }
        /****************** 以下是关于BlobFileFooter的相关成员函数 ******************/
        /**
         * 将footer编码到dst中
         * @param dst 目标位置
         * @details
         * 需要注意的是，几个成员当中，只有meta_index_handle(BlockHandle类型)因为是使用两个varint来构建的。
         * 所以就需要单独加入一些padding来使得整个footer的大小为一个固定值，方便解析？
         */
        void BlobFileFooter::EncodeTo(std::string *dst) const
        {
            auto size = dst->size();
            meta_index_handle.EncodeTo(dst);
            // Add padding to make a fixed size footer.
            dst->resize(size + kEncodedLength - 12);
            PutFixed64(dst, kFooterMagicNumber);
            Slice encoded(dst->data() + size, dst->size() - size);
            PutFixed32(dst, crc32c::Value(encoded.data(), encoded.size()));
        }

        Status BlobFileFooter::DecodeFrom(Slice *src)
        {
            auto data = src->data();
            Status s = meta_index_handle.DecodeFrom(src);
            if (!s.ok())
            {
                return Status::Corruption("BlobFileFooter", s.ToString());
            }
            // Remove padding.
            src->remove_prefix(data + kEncodedLength - 12 - src->data());
            uint64_t magic_number = 0;
            if (!GetFixed64(src, &magic_number) || magic_number != kFooterMagicNumber)
            {
                return Status::Corruption("BlobFileFooter", "magic number");
            }
            Slice decoded(data, src->data() - data);
            uint32_t checksum = 0;
            if (!GetFixed32(src, &checksum) ||
                crc32c::Value(decoded.data(), decoded.size()) != checksum)
            {
                return Status::Corruption("BlobFileFooter", "checksum");
            }
            return Status::OK();
        }

        bool operator==(const BlobFileFooter &lhs, const BlobFileFooter &rhs)
        {
            return (lhs.meta_index_handle.offset() == rhs.meta_index_handle.offset() &&
                    lhs.meta_index_handle.size() == rhs.meta_index_handle.size());
        }

    } // namespace titandb
} // namespace rocksdb
