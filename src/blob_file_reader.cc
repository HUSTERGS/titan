#include "blob_file_reader.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "file/filename.h"
#include "table/block_based/block.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"
#include "titan_stats.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace rocksdb
{
    namespace titandb
    {
        /**
         * 创建一个新的Blob File Reader
         *
         * @param file_number 目标blob文件编号，用于拼接成具体文件名
         * @param readahead_size 预读取大小？不知道有什么用
         * @param db_options 数据库配置
         * @param env_options 环境配置
         * @param env
         * @param result 一个RandomAccessFileReader的指针，用于保存结果，也就是一个reader
         * @return
         */
        Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                                 const TitanDBOptions &db_options,
                                 const EnvOptions &env_options, Env *env,
                                 std::unique_ptr<RandomAccessFileReader> *result)
        {
            std::unique_ptr<RandomAccessFile> file;
            // 根据配置中的文件夹以及具体的文件编号来获得blob文件名，本质上就是简单的字符拼接
            auto file_name = BlobFileName(db_options.dirname, file_number);
            Status s = env->NewRandomAccessFile(file_name, &file, env_options);
            if (!s.ok())
                return s;
            // readahead参数是什么
            if (readahead_size > 0)
            {
                file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
            }
            result->reset(new RandomAccessFileReader(
                std::move(file), file_name, nullptr /*env*/, nullptr /*stats*/,
                0 /*hist_type*/, nullptr /*file_read_hist*/, env_options.rate_limiter));
            return s;
        }

        const uint64_t kMaxReadaheadSize = 256 << 10;

        namespace
        {
            /**
             * 将获得的文件唯一id保存在dst中，不是很懂这个地方和cache是什么关系
             * 似乎就是直接获取文件对应的ID，并没有额外的计算工作，每一个文件以randomAccessFile打开的时候都会有一个唯一的ID似乎
             * @param dst 目标位置
             * @param cc
             * @param file
             */
            void GenerateCachePrefix(std::string *dst, Cache *cc, RandomAccessFile *file)
            {
                char buffer[kMaxVarint64Length * 3 + 1];
                // 似乎就是直接获取文件对应的ID，并没有额外的计算工作，每一个文件以randomAccessFile打开的时候都会有一个唯一的ID似乎
                auto size = file->GetUniqueId(buffer, sizeof(buffer));
                // 这个地方很奇怪，如果size=0，说明没有成功获得UniqueId，所以接下来就从cache中获得？
                // 而且后续也没有在file中更新，如果多次打开同一个文件会导致每一次的UniqueId不一样吗？
                // QUES: placeholder
                if (size == 0)
                {
                    auto end = EncodeVarint64(buffer, cc->NewId());
                    size = end - buffer;
                }
                dst->assign(buffer, size);
            }
            
            /**
             * 分别将prefix以及offset放到dst中，其中offset以varint的方式存放
             * @param dst 目标位置
             * @param prefix 前缀
             * @param offset ~~不知道是什么，会一并存放在dst中~~，目标数据的偏移量
             * @details
             * 这个函数似乎只用于由BlobHandle生成对应的cache_key，也就是，当我们尝试通过一个BlobHandle来在cache中查询是否有
             * 目标record的时候，作为查找的key。其中prefix就是cache_prefix，应该是某一个文件的prefix都是独一无二的，然后再加上
             * record在文件中的偏移，也就是offset参数，同时也是BlobHandle中的offset成员，共同唯一确定了一个record记录。cache
             * 应该并没有对文件进行区分，所有文件应该共用了cache
             */
            void EncodeBlobCache(std::string *dst, const Slice &prefix, uint64_t offset)
            {
                dst->assign(prefix.data(), prefix.size());
                PutVarint64(dst, offset);
            }



            // Seek to the specified meta block.
            // Return true if it successfully seeks to that block.
            // 如果找到对应的block，通过InternalIterator，并且在block_handle重置为找到的block的数据
            /**
             * 不会用到，先不看了
             * @param meta_iter
             * @param block_name
             * @param is_found
             * @param block_handle
             * @return
             */
            Status SeekToMetaBlock(InternalIterator *meta_iter,
                                   const std::string &block_name, bool *is_found,
                                   BlockHandle *block_handle = nullptr)
            {
                // 如果block_handle不是空那么就将其设置为空
                if (block_handle != nullptr)
                {
                    *block_handle = BlockHandle::NullBlockHandle();
                }
                *is_found = false;
                meta_iter->Seek(block_name);
                if (meta_iter->status().ok() && meta_iter->Valid() &&
                    meta_iter->key() == block_name)
                {
                    *is_found = true;
                    if (block_handle)
                    {
                        Slice v = meta_iter->value();
                        return block_handle->DecodeFrom(&v);
                    }
                }
                return meta_iter->status();
            }

        } // namespace
        /**************** 以下是BlobFileReader相关代码 **********/

        /**
         * 打开某一个文件，将其内容包装为一个reader，作为result返回
         * 如果有压缩数据的话，就进行解析并保存在reader的uncompression_dict_字段当中
         * @param options
         * @param file
         * @param file_size
         * @param result
         * @param stats
         * @return
         */
        Status BlobFileReader::Open(const TitanCFOptions &options,
                                    std::unique_ptr<RandomAccessFileReader> file,
                                    uint64_t file_size,
                                    std::unique_ptr<BlobFileReader> *result,
                                    TitanStats *stats)
        {
            // 首先判断文件大小是否符合最小要求
            if (file_size < BlobFileFooter::kEncodedLength)
            {
                return Status::Corruption("file is too short to be a blob file");
            }
            // 分别获得头部，获得头部是为了知道是否进行了压缩？
            BlobFileHeader header;
            Status s = ReadHeader(file, &header);
            if (!s.ok())
            {
                return s;
            }
            // 以及footer，获得尾部是为了获取压缩数据的存放地址？
            FixedSlice<BlobFileFooter::kEncodedLength> buffer;
            s = file->Read(file_size - BlobFileFooter::kEncodedLength,
                           BlobFileFooter::kEncodedLength, &buffer, buffer.get());
            if (!s.ok())
            {
                return s;
            }

            BlobFileFooter footer;
            s = DecodeInto(buffer, &footer);
            if (!s.ok())
            {
                return s;
            }

            auto reader = new BlobFileReader(options, std::move(file), stats);
            reader->footer_ = footer;

            if (header.flags & BlobFileHeader::kHasUncompressionDictionary)
            {
                s = InitUncompressionDict(footer, reader->file_.get(),
                                          &reader->uncompression_dict_);
                if (!s.ok())
                {
                    return s;
                }
            }
            // reset方法将result重新设置为reader，读到的数据都被保存在reader的成员中
            result->reset(reader);
            return Status::OK();
        }

        /**
         * 读取blob文件的header，保存在header参数中
         * @param file 目标文件
         * @param header 结果保存
         * @return 是否成功
         */
        Status BlobFileReader::ReadHeader(std::unique_ptr<RandomAccessFileReader> &file,
                                          BlobFileHeader *header)
        {
            FixedSlice<BlobFileHeader::kMaxEncodedLength> buffer;
            Status s =
                file->Read(0, BlobFileHeader::kMaxEncodedLength, &buffer, buffer.get());
            if (!s.ok())
                return s;

            s = DecodeInto(buffer, header);

            return s;
        }

        /**
         * 构造函数，如果option中有cache选项，那么就尝试生成一个缓存前缀，保存在cache_prefix_成员当中
         * @param options
         * @param file
         * @param stats
         */
        BlobFileReader::BlobFileReader(const TitanCFOptions &options,
                                       std::unique_ptr<RandomAccessFileReader> file,
                                       TitanStats *stats)
            : options_(options),
              file_(std::move(file)),
              cache_(options.blob_cache),
              stats_(stats)
        {
            if (cache_)
            {
                GenerateCachePrefix(&cache_prefix_, cache_.get(), file_->file());
            }
        }

        /**
         * 尝试通过blobhandle获取record，如果可以从缓存中获取则直接从缓存中获取，如果没有命中则直接读取并进行缓存的更新
         * @param handle 指向目标blob数据
         * @param record 用于保存找到的record
         * @param buffer
         * @return
         */
        Status BlobFileReader::Get(const ReadOptions & /*options*/,
                                   const BlobHandle &handle, BlobRecord *record,
                                   PinnableSlice *buffer)
        {
            TEST_SYNC_POINT("BlobFileReader::Get");

            std::string cache_key;
            Cache::Handle *cache_handle = nullptr;
            if (cache_)
            {
                // 通过拼接cache_prefix_以及offset来生成cache查找的key
                EncodeBlobCache(&cache_key, cache_prefix_, handle.offset);
                // 先在cache中寻找对应的数据，如果找到的话就直接返回
                cache_handle = cache_->Lookup(cache_key);
                if (cache_handle)
                {
                    RecordTick(statistics(stats_), TITAN_BLOB_CACHE_HIT);
                    auto blob = reinterpret_cast<OwnedSlice *>(cache_->Value(cache_handle));
                    buffer->PinSlice(*blob, UnrefCacheHandle, cache_.get(), cache_handle);
                    return DecodeInto(*blob, record);
                }
            }
            RecordTick(statistics(stats_), TITAN_BLOB_CACHE_MISS);

            OwnedSlice blob;
            Status s = ReadRecord(handle, record, &blob);
            if (!s.ok())
            {
                return s;
            }
            // 如果没有cache那么就向cache中插入新数据
            // QUES: 不懂下面的这些PinSlice都是些什么
            if (cache_)
            {
                auto cache_value = new OwnedSlice(std::move(blob));
                auto cache_size = cache_value->size() + sizeof(*cache_value);
                cache_->Insert(cache_key, cache_value, cache_size,
                               &DeleteCacheValue<OwnedSlice>, &cache_handle);
                buffer->PinSlice(*cache_value, UnrefCacheHandle, cache_.get(),
                                 cache_handle);
            }
            else
            {
                buffer->PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
            }

            return Status::OK();
        }


        /**
         * 直接通过blobhandle获得对应的record
         * @param handle
         * @param record
         * @param buffer
         * @return
         */
        Status BlobFileReader::ReadRecord(const BlobHandle &handle, BlobRecord *record,
                                          OwnedSlice *buffer)
        {
            Slice blob;
            CacheAllocationPtr ubuf(new char[handle.size]);
            Status s = file_->Read(handle.offset, handle.size, &blob, ubuf.get());
            if (!s.ok())
            {
                return s;
            }
            if (handle.size != static_cast<uint64_t>(blob.size()))
            {
                return Status::Corruption(
                    "ReadRecord actual size: " + ToString(blob.size()) +
                    " not equal to blob size " + ToString(handle.size));
            }

            BlobDecoder decoder(uncompression_dict_ == nullptr
                                    ? &UncompressionDict::GetEmptyDict()
                                    : uncompression_dict_.get());
            s = decoder.DecodeHeader(&blob);
            if (!s.ok())
            {
                return s;
            }
            buffer->reset(std::move(ubuf), blob);
            s = decoder.DecodeRecord(&blob, record, buffer);
            return s;
        }
        /**
         * 用到的应该比较少，看到了的话建议直接看函数里面的注释
         * @param options
         * @param handle
         * @param record
         * @param buffer
         * @return
         */
        Status BlobFilePrefetcher::Get(const ReadOptions &options,
                                       const BlobHandle &handle, BlobRecord *record,
                                       PinnableSlice *buffer)
        {
            // 这个if用于判断是否在顺序读取，每一次顺序读取都会进一步增大readahead_size
            if (handle.offset == last_offset_)
            {
                last_offset_ = handle.offset + handle.size;
                if (handle.offset + handle.size > readahead_limit_)
                {
                    readahead_size_ = std::max(handle.size, readahead_size_);
                    // 具体的预读似乎就是将文件指定位置的数据先进行读取并存放到缓存中
                    // QUES: 这一部分可以细看一点
                    reader_->file_->Prefetch(handle.offset, readahead_size_);
                    readahead_limit_ = handle.offset + readahead_size_;
                    // 增加预读大小，每次加倍，但是不能超过设定的最大值
                    readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
                }
            }
            else
            {
                // 一旦检测到没有顺序读了，就对readahead_size以及readahead_limit进行重置
                last_offset_ = handle.offset + handle.size;
                readahead_size_ = 0;
                readahead_limit_ = 0;
            }

            return reader_->Get(options, handle, record, buffer);
        }
        // 懒得看了，可以看函数里面的注释
        Status InitUncompressionDict(
            const BlobFileFooter &footer, RandomAccessFileReader *file,
            std::unique_ptr<UncompressionDict> *uncompression_dict)
        {
            // TODO: Cache the compression dictionary in either block cache or blob cache.
#if ZSTD_VERSION_NUMBER < 10103
            return Status::NotSupported("the version of libztsd is too low");
#endif
            // 1. read meta index block
            // 2. read dictionary
            // 3. reset the dictionary
            assert(footer.meta_index_handle.size() > 0);
            BlockHandle meta_index_handle = footer.meta_index_handle;
            Slice blob;
            CacheAllocationPtr ubuf(new char[meta_index_handle.size()]);
            Status s = file->Read(meta_index_handle.offset(), meta_index_handle.size(),
                                  &blob, ubuf.get());
            if (!s.ok())
            {
                return s;
            }
            BlockContents meta_block_content(std::move(ubuf), meta_index_handle.size());

            std::unique_ptr<Block> meta(
                new Block(std::move(meta_block_content), kDisableGlobalSequenceNumber));

            std::unique_ptr<InternalIterator> meta_iter(
                meta->NewDataIterator(BytewiseComparator(), BytewiseComparator()));

            bool dict_is_found = false;
            BlockHandle dict_block;
            s = SeekToMetaBlock(meta_iter.get(), kCompressionDictBlock, &dict_is_found,
                                &dict_block);
            if (!s.ok())
            {
                return s;
            }

            if (!dict_is_found)
            {
                return Status::NotFound("uncompression dict");
            }

            Slice dict_slice;
            CacheAllocationPtr dict_buf(new char[dict_block.size()]);
            s = file->Read(dict_block.offset(), dict_block.size(), &dict_slice,
                           dict_buf.get());
            if (!s.ok())
            {
                return s;
            }

            std::string dict_str(dict_buf.get(), dict_buf.get() + dict_block.size());
            uncompression_dict->reset(new UncompressionDict(dict_str, true));

            return s;
        }

    } // namespace titandb
} // namespace rocksdb
