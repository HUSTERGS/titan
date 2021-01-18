#include "blob_file_cache.h"

#include "file/filename.h"
#include "util.h"

namespace rocksdb
{
    namespace titandb
    {

        namespace
        {
            // 将文件编号从数字编码为字符
            Slice EncodeFileNumber(const uint64_t *number)
            {
                return Slice(reinterpret_cast<const char *>(number), sizeof(*number));
            }

        } // namespace

        BlobFileCache::BlobFileCache(const TitanDBOptions &db_options,
                                     const TitanCFOptions &cf_options,
                                     std::shared_ptr<Cache> cache, TitanStats *stats)
            : env_(db_options.env),
              env_options_(db_options),
              db_options_(db_options),
              cf_options_(cf_options),
              cache_(cache),
              stats_(stats) {}
        /**
         * 尝试通过blobhandle获取blobrecord，存放在record中，如果在缓存中有的话那么直接从缓存中获取，如果没有命中缓存就进行相关的操作
         * @param options
         * @param file_number
         * @param file_size
         * @param handle
         * @param record
         * @param buffer
         * @return
         */
        Status BlobFileCache::Get(const ReadOptions &options, uint64_t file_number,
                                  uint64_t file_size, const BlobHandle &handle,
                                  BlobRecord *record, PinnableSlice *buffer)
        {
            Cache::Handle *cache_handle = nullptr;
            // 获取cache相关信息
            Status s = FindFile(file_number, file_size, &cache_handle);
            if (!s.ok())
                return s;

            auto reader = reinterpret_cast<BlobFileReader *>(cache_->Value(cache_handle));
            s = reader->Get(options, handle, record, buffer);
            cache_->Release(cache_handle);
            return s;
        }
        // 返回一个BlobFilePrefetcher
        Status BlobFileCache::NewPrefetcher(
            uint64_t file_number, uint64_t file_size,
            std::unique_ptr<BlobFilePrefetcher> *result)
        {
            Cache::Handle *cache_handle = nullptr;
            Status s = FindFile(file_number, file_size, &cache_handle);
            if (!s.ok())
                return s;

            auto reader = reinterpret_cast<BlobFileReader *>(cache_->Value(cache_handle));
            auto prefetcher = new BlobFilePrefetcher(reader);
            // QUES: 下面这个没看懂
            prefetcher->RegisterCleanup(&UnrefCacheHandle, cache_.get(), cache_handle);
            result->reset(prefetcher);
            return s;
        }
        /**
         * 将某一个文件编号从缓存中去除出去（因为cache中的key就是文件编号）
         * @param file_number 需要去除的文件编号
         */
        void BlobFileCache::Evict(uint64_t file_number)
        {
            cache_->Erase(EncodeFileNumber(&file_number));
        }
        /**
         * 通过file_number获取cache的句柄，也就是找到对应文件的cache相关信息
         * @param file_number 文件编号，将转化为字符串直接作为在cache中索引的key
         * @param file_size 文件大小
         * @param handle 不知道是什么，应该是用于存放返回值？如果在缓存中找到了，那么就会被直接赋值，如果没有找到就会在
         * 向缓存中插入新的条目的时候进行赋值，可以通过`cache_->Value(handle)`来获得实际的`BlobFileReader *`类型的值
         * @return
         */
        Status BlobFileCache::FindFile(uint64_t file_number, uint64_t file_size,
                                       Cache::Handle **handle)
        {
            Status s;
            Slice cache_key = EncodeFileNumber(&file_number);
            // 其实不是很懂这个rocksdb内置的cache是怎么实现的，类似于map吗？
            // 似乎是基于hash值的map，然后cache使用的是LRU
            // handle的key是cache_key，这里是文件编号
            *handle = cache_->Lookup(cache_key);
            // 如果找到了就直接返回
            if (*handle)
            {
                // TODO: add file reader cache hit/miss metrics
                return s;
            }
            std::unique_ptr<RandomAccessFileReader> file;
            {
                std::unique_ptr<RandomAccessFile> f;
                // 简单的字符拼接
                auto file_name = BlobFileName(db_options_.dirname, file_number);
                s = env_->NewRandomAccessFile(file_name, &f, env_options_);
                if (!s.ok())
                    return s;
                // 如果是随机访问的文件，比如ssd，那么就会将f标记为随机访问
                if (db_options_.advise_random_on_open)
                {
                    f->Hint(RandomAccessFile::RANDOM);
                }
                file.reset(new RandomAccessFileReader(std::move(f), file_name));
            }

            std::unique_ptr<BlobFileReader> reader;
            s = BlobFileReader::Open(cf_options_, std::move(file), file_size, &reader,
                                     stats_);
            if (!s.ok())
                return s;
            // 由于没有命中所以需要更新cache
            cache_->Insert(cache_key, reader.release(), 1,
                           &DeleteCacheValue<BlobFileReader>, handle);
            return s;
        }

    } // namespace titandb
} // namespace rocksdb
