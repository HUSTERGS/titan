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
            // 将获得的文件唯一id保存在dst中
            void GenerateCachePrefix(std::string *dst, Cache *cc, RandomAccessFile *file)
            {
                char buffer[kMaxVarint64Length * 3 + 1];
                // 似乎就是直接获取文件对应的ID，并没有额外的计算工作，每一个文件以randomAccessFile打开的时候都会有一个唯一的ID似乎
                auto size = file->GetUniqueId(buffer, sizeof(buffer));
                if (size == 0)
                {
                    auto end = EncodeVarint64(buffer, cc->NewId());
                    size = end - buffer;
                }
                dst->assign(buffer, size);
            }
            
            // 分别将prefix以及offset放到dst中
            void EncodeBlobCache(std::string *dst, const Slice &prefix, uint64_t offset)
            {
                dst->assign(prefix.data(), prefix.size());
                PutVarint64(dst, offset);
            }

            // Seek to the specified meta block.
            // Return true if it successfully seeks to that block.
            // 如果找到对应的block，通过InternalIterator，并且在block_handle重置为找到的block的数据
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
        // 打开某一个文件，将其内容包装为一个reader，作为result返回
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
            // 分别获得头部
            BlobFileHeader header;
            Status s = ReadHeader(file, &header);
            if (!s.ok())
            {
                return s;
            }
            // 以及footer
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
        // 读取文件头保存在buffer中
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
        // 构造函数，如果cache已经存在的话那么就获得其cache_prefix
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
        
        // 尝试通过blobhandle获取record，如果可以从缓存中获取则直接从缓存中获取，如果没有命中则直接读取并进行缓存的更新
        Status BlobFileReader::Get(const ReadOptions & /*options*/,
                                   const BlobHandle &handle, BlobRecord *record,
                                   PinnableSlice *buffer)
        {
            TEST_SYNC_POINT("BlobFileReader::Get");

            std::string cache_key;
            Cache::Handle *cache_handle = nullptr;
            if (cache_)
            {
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
        // 直接通过blobhandle获得对应的record
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
        // 没搞懂，妈的
        Status BlobFilePrefetcher::Get(const ReadOptions &options,
                                       const BlobHandle &handle, BlobRecord *record,
                                       PinnableSlice *buffer)
        {
            if (handle.offset == last_offset_)
            {
                last_offset_ = handle.offset + handle.size;
                if (handle.offset + handle.size > readahead_limit_)
                {
                    readahead_size_ = std::max(handle.size, readahead_size_);
                    reader_->file_->Prefetch(handle.offset, readahead_size_);
                    readahead_limit_ = handle.offset + readahead_size_;
                    readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
                }
            }
            else
            {
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
