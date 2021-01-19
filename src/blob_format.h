/**
 * 宏`#pragma once`与`#ifndef/#endif`功能类似，
 * 用于保证文件只被编译一次，也就是保证文件之间相互include的时候不会出错。
 * 前者的可移植性可能会差一些，但是能够完全消除重名的情况，因为判断是否重复的依据是"物理"上的文件，而不是定义的宏名
 * 而后者还是需要担心是否出现重名
 */
#pragma once

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "table/format.h"
#include "util.h"

namespace rocksdb
{
    namespace titandb
    {
        /**
         * blob文件的格式与leveldb中的sst文件似乎很像
         * [blob header] + [header-list] + [meta-list] + [meta index] + [blob footer]
         * titan中的meta似乎并没有真正使用，只是作为可选的拓展？
         */
        // Blob file overall format:
        //
        // [blob file header]
        // [record head + record 1]
        // [record head + record 2]
        // ...
        // [record head + record N]
        // [blob file meta block 1]
        // [blob file meta block 2]
        // ...
        // [blob file meta block M]
        // [blob file meta index]
        // [blob file footer]
        //
        // For now, the only kind of meta block is an optional uncompression dictionary
        // indicated by a flag in the file header.
        // 目前唯一的元数据块就是一可选的、未经压缩的字典，在文件头中标明
        // blob头的格式
        // 一个blob文件对应一个blob头，
        // Format of blob head (9 bytes):
        //
        //    +---------+---------+-------------+
        //    |   crc   |  size   | compression |
        //    +---------+---------+-------------+
        //    | Fixed32 | Fixed32 |    char     |
        //    +---------+---------+-------------+
        //
        /**
         * QUES: 为何这个长度和上面的head长度不一致，上面说的是`Format of blob head(9 bytes)`，
         * 但是下面说的长度为9的是`kRecordHeaderSize`，所以上面的硬指的是一个record的header而不是整个blob文件的header？
         */
        const uint64_t kBlobMaxHeaderSize = 12;
        const uint64_t kRecordHeaderSize = 9;
        const uint64_t kBlobFooterSize = BlockHandle::kMaxEncodedLength + 8 + 4;  // ?
        //
        /**
         * QUES: 为何len在后面？
         * ANS: 下面这个其实有问题，实际存储的时候是像下面这样的
         * +--------------------------------+------------------------------------+
         * |              key               |                value               |
         * +--------------------------------+------------------------------------+
         * | key_len(varint) + key(fixed)   |  value_len(varint) + value(fixed)  |
         * +--------------------------------+------------------------------------+
         */
        // Format of blob record (not fixed size):
        //
        //    +--------------------+----------------------+
        //    |        key         |        value         |
        //    +--------------------+----------------------+
        //    | Varint64 + key_len | Varint64 + value_len |
        //    +--------------------+----------------------+
        //
        /**
         * 一个Blob Record的数据，一个record就是一个key + value，使用Slice存储
         */
        struct BlobRecord
        {
            Slice key;
            Slice value;

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);

            size_t size() const { return key.size() + value.size(); }

            friend bool operator==(const BlobRecord &lhs, const BlobRecord &rhs);
        };
        /**
         * 用于encode一个单独的record记录
         * 最终encode的结果会保存在record_以及header_成员中
         * 没有选择保存在外部传入的buffer中，可能是之后还有别的操作吧
         */
        class BlobEncoder
        {
        public:
            BlobEncoder(CompressionType compression, CompressionOptions compression_opt,
                        const CompressionDict *compression_dict)
                : compression_opt_(compression_opt),
                  compression_ctx_(compression),
                  compression_dict_(compression_dict),
                  compression_info_(new CompressionInfo(
                      compression_opt_, compression_ctx_, *compression_dict_, compression,
                      0 /*sample_for_compression*/)) {}
            BlobEncoder(CompressionType compression)
                : BlobEncoder(compression, CompressionOptions(),
                              &CompressionDict::GetEmptyDict()) {}
            BlobEncoder(CompressionType compression,
                        const CompressionDict *compression_dict)
                : BlobEncoder(compression, CompressionOptions(), compression_dict) {}
            BlobEncoder(CompressionType compression, CompressionOptions compression_opt)
                : BlobEncoder(compression, compression_opt,
                              &CompressionDict::GetEmptyDict()) {}

            void EncodeRecord(const BlobRecord &record);
            void EncodeSlice(const Slice &record);
            void SetCompressionDict(const CompressionDict *compression_dict)
            {
                compression_dict_ = compression_dict;
                compression_info_.reset(new CompressionInfo(
                    compression_opt_, compression_ctx_, *compression_dict_,
                    compression_info_->type(), compression_info_->SampleForCompression()));
            }

            Slice GetHeader() const { return Slice(header_, sizeof(header_)); }
            Slice GetRecord() const { return record_; }

            size_t GetEncodedSize() const { return sizeof(header_) + record_.size(); }

        private:
            char header_[kRecordHeaderSize]; // 保存一个record的header
            Slice record_; // 存放Slice压缩好的`数据部分`
            std::string record_buffer_; //用于临时保存record本身encode之后的Slice
            std::string compressed_buffer_; // 以下都是压缩相关的
            CompressionOptions compression_opt_;
            CompressionContext compression_ctx_;
            const CompressionDict *compression_dict_;
            std::unique_ptr<CompressionInfo> compression_info_;
        };

        /**
         * 将数据最终解析为BlobRecord(key + value)
         */
        class BlobDecoder
        {
        public:
            BlobDecoder(const UncompressionDict *uncompression_dict,
                        CompressionType compression = kNoCompression)
                : compression_(compression), uncompression_dict_(uncompression_dict) {}

            BlobDecoder()
                : BlobDecoder(&UncompressionDict::GetEmptyDict(), kNoCompression) {}

            Status DecodeHeader(Slice *src);
            Status DecodeRecord(Slice *src, BlobRecord *record, OwnedSlice *buffer);

            void SetUncompressionDict(const UncompressionDict *uncompression_dict)
            {
                uncompression_dict_ = uncompression_dict;
            }

            size_t GetRecordSize() const { return record_size_; }

        private:
            uint32_t crc_{0};
            uint32_t header_crc_{0};
            uint32_t record_size_{0};
            CompressionType compression_{kNoCompression};
            const UncompressionDict *uncompression_dict_;
        };
        // offset即为文件中的偏移量，而size表示从此偏移量开始的大小
        // Format of blob handle (not fixed size):
        //
        //    +----------+----------+
        //    |  offset  |   size   |
        //    +----------+----------+
        //    | Varint64 | Varint64 |
        //    +----------+----------+
        // 可以将其理解为一个Blob指针？指向了某一个数据段
        struct BlobHandle
        {
            uint64_t offset{0};
            uint64_t size{0};

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);

            friend bool operator==(const BlobHandle &lhs, const BlobHandle &rhs);
        };

        // Format of blob index (not fixed size):
        //
        //    +------+-------------+------------------------------------+
        //    | type | file number |            blob handle             |
        //    +------+-------------+------------------------------------+
        //    | char |  Varint64   | Varint64(offsest) + Varint64(size) |
        //    +------+-------------+------------------------------------+
        //
        // It is stored in LSM-Tree as the value of key, then Titan can use this blob
        // index to locate actual value from blob file.
        // 存储在LSM-Tree中作为key对应的value
        // blob handle加上文件编号之后，形成了三元组（file-number offset size）可以用来定位某一个文件中的一个数据段
        // 这里就是指向了一个BlobRecord？
        struct BlobIndex
        {
            enum Type : unsigned char
            {
                kBlobRecord = 1,
            };
            uint64_t file_number{0}; // 如果是0说明是deletion marker
            BlobHandle blob_handle;

            virtual ~BlobIndex() {}

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);
            static void EncodeDeletionMarkerTo(std::string *dst);
            static bool IsDeletionMarker(const BlobIndex &index);

            bool operator==(const BlobIndex &rhs) const;
        };


        // 完全不懂是用来干嘛的
        struct MergeBlobIndex : public BlobIndex
        {
            uint64_t source_file_number{0};
            uint64_t source_file_offset{0};

            void EncodeTo(std::string *dst) const;
            void EncodeToBase(std::string *dst) const;
            Status DecodeFrom(Slice *src);
            Status DecodeFromBase(Slice *src);

            bool operator==(const MergeBlobIndex &rhs) const;
        };







        // 存放在manifest文件当中的数据。不是存放在blob中的数据
        // Format of blob file meta (not fixed size):
        //
        //    +-------------+-----------+--------------+------------+
        //    | file number | file size | file entries | file level |
        //    +-------------+-----------+--------------+------------+
        //    |  Varint64   | Varint64  |   Varint64   |  Varint32  |
        //    +-------------+-----------+--------------+------------+
        //    +--------------------+--------------------+
        //    |    smallest key    |    largest key     |
        //    +--------------------+--------------------+
        //    | Varint32 + key_len | Varint32 + key_len |
        //    +--------------------+--------------------+
        //
        // The blob file meta is stored in Titan's manifest for quick constructing of
        // meta infomations of all the blob files in memory.
        //
        // Legacy format:
        //
        //    +-------------+-----------+
        //    | file number | file size |
        //    +-------------+-----------+
        //    |  Varint64   | Varint64  |
        //    +-------------+-----------+
        //
        class BlobFileMeta
        {
        public:
            enum class FileEvent : int
            {
                kInit,
                kFlushCompleted,
                kCompactionCompleted,
                kGCCompleted,
                kGCBegin,
                kGCOutput,
                kFlushOrCompactionOutput,
                kDbRestart,
                kDelete,
                kNeedMerge,
                kReset, // reset file to normal for test
            };

            enum class FileState : int
            {
                kInit, // file never at this state
                kNormal,
                kPendingLSM, // waiting keys adding to LSM
                kBeingGC,    // being gced
                kPendingGC,  // output of gc, waiting gc finish and keys adding to LSM
                kObsolete,   // already gced, but wait to be physical deleted
                kToMerge,    // need merge to new blob file in next compaction
            };

            BlobFileMeta() = default;

            BlobFileMeta(uint64_t _file_number, uint64_t _file_size,
                         uint64_t _file_entries, uint32_t _file_level,
                         const std::string &_smallest_key,
                         const std::string &_largest_key)
                : file_number_(_file_number),
                  file_size_(_file_size),
                  file_entries_(_file_entries),
                  file_level_(_file_level),
                  smallest_key_(_smallest_key),
                  largest_key_(_largest_key) {}

            friend bool operator==(const BlobFileMeta &lhs, const BlobFileMeta &rhs);

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);
            Status DecodeFromLegacy(Slice *src);

            uint64_t file_number() const { return file_number_; }
            uint64_t file_size() const { return file_size_; }
            uint64_t live_data_size() const { return live_data_size_; }
            uint32_t file_level() const { return file_level_; }
            const std::string &smallest_key() const { return smallest_key_; }
            const std::string &largest_key() const { return largest_key_; }

            void set_live_data_size(uint64_t size) { live_data_size_ = size; }
            uint64_t file_entries() const { return file_entries_; }
            FileState file_state() const { return state_; }
            bool is_obsolete() const { return state_ == FileState::kObsolete; }

            void FileStateTransit(const FileEvent &event);
            bool UpdateLiveDataSize(int64_t delta)
            {
                int64_t result = static_cast<int64_t>(live_data_size_) + delta;
                if (result < 0)
                {
                    live_data_size_ = 0;
                    return false;
                }
                live_data_size_ = static_cast<uint64_t>(result);
                return true;
            }
            bool NoLiveData() { return live_data_size_ == 0; }
            /**
             * 计算得到可以丢弃的数据比例
             * 减去blobheader以及footer之后，live_data_size_所占的比例
             * 其实感觉这个地方，怎么说，可以进行微调？
             * 作者没有将metablock的空间也去除，因为metablock实际上并没有实现
             * @return
             */
            double GetDiscardableRatio() const
            {
                if (file_size_ == 0)
                {
                    return 0;
                }
                // TODO: Exclude meta blocks from file size
                return 1 - (static_cast<double>(live_data_size_) /
                            (file_size_ - kBlobMaxHeaderSize - kBlobFooterSize));
            }
            TitanInternalStats::StatsType GetDiscardableRatioLevel() const;

        private:
            // Persistent field

            uint64_t file_number_{0};
            uint64_t file_size_{0};
            // QUES: 不懂是什么
            uint64_t file_entries_;
            // Target level of compaction/flush which generates this blob file
            // 生成当前blob文件的压缩或者刷新操作的目标level（在LSM树中）
            uint32_t file_level_;
            // Empty `smallest_key_` and `largest_key_` means smallest key is unknown,
            // and can only happen when the file is from legacy version.
            std::string smallest_key_;
            std::string largest_key_;

            // Not persistent field

            // Size of data with reference from SST files.
            //
            // Because the new generated SST is added to superversion before
            // `OnFlushCompleted()`/`OnCompactionCompleted()` is called, so if there is a
            // later compaction trigger by the new generated SST, the later
            // `OnCompactionCompleted()` maybe called before the previous events'
            // `OnFlushCompleted()`/`OnCompactionCompleted()` is called.
            // So when state_ == kPendingLSM, it uses this to record the delta as a
            // positive number if any later compaction is trigger before previous
            // `OnCompactionCompleted()` is called.
            std::atomic<uint64_t> live_data_size_{0};
            std::atomic<FileState> state_{FileState::kInit};
        };





        // Format of blob file header for version 1 (8 bytes):
        //
        //    +--------------+---------+
        //    | magic number | version |
        //    +--------------+---------+
        //    |   Fixed32    | Fixed32 |
        //    +--------------+---------+
        //
        // For version 2, there are another 4 bytes for flags:
        //
        //    +--------------+---------+---------+
        //    | magic number | version |  flags  |
        //    +--------------+---------+---------+
        //    |   Fixed32    | Fixed32 | Fixed32 |
        //    +--------------+---------+---------+
        //
        // The header is mean to be compatible with header of BlobDB blob files, except
        // we use a different magic number.
        //
        // blob`文件`的头部
        struct BlobFileHeader
        {
            // The first 32bits from $(echo titandb/blob | sha1sum).
            static const uint32_t kHeaderMagicNumber = 0x2be0a614ul;
            static const uint32_t kVersion1 = 1;
            static const uint32_t kVersion2 = 2;

            static const uint64_t kMinEncodedLength = 4 + 4;
            static const uint64_t kMaxEncodedLength = 4 + 4 + 4;

            // Flags:
            static const uint32_t kHasUncompressionDictionary = 1 << 0;

            uint32_t version = kVersion2;
            uint32_t flags = 0;

            uint64_t size() const
            {
                return version == BlobFileHeader::kVersion1
                           ? BlobFileHeader::kMinEncodedLength
                           : BlobFileHeader::kMaxEncodedLength;
            }

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);
        };

        // Format of blob file footer (BlockHandle::kMaxEncodedLength + 12):
        //
        //    +---------------------+-------------+--------------+----------+
        //    |  meta index handle  |   padding   | magic number | checksum |
        //    +---------------------+-------------+--------------+----------+
        //    | Varint64 + Varint64 | padding_len |   Fixed64    | Fixed32  |
        //    +---------------------+-------------+--------------+----------+
        //
        // To make the blob file footer fixed size,
        // the padding_len is `BlockHandle::kMaxEncodedLength - meta_handle_len`
        struct BlobFileFooter
        {
            // The first 64bits from $(echo titandb/blob | sha1sum).
            static const uint64_t kFooterMagicNumber{0x2be0a6148e39edc6ull};
            static const uint64_t kEncodedLength{kBlobFooterSize};

            BlockHandle meta_index_handle{BlockHandle::NullBlockHandle()};

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);

            friend bool operator==(const BlobFileFooter &lhs, const BlobFileFooter &rhs);
        };

        // A convenient template to decode a const slice.
        // 会通过调用参数target的DecodeFrom方法来将src中的数据解析道target中
        template <typename T>
        Status DecodeInto(const Slice &src, T *target)
        {
            auto tmp = src;
            auto s = target->DecodeFrom(&tmp);
            if (s.ok() && !tmp.empty())
            {
                s = Status::Corruption(Slice());
            }
            return s;
        }

    } // namespace titandb
} // namespace rocksdb
