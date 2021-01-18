#include "version_edit.h"

#include "util/coding.h"

namespace rocksdb {
    namespace titandb {
        /**
         * 将VersionEdit保存在dst当中
         * 与其他的数据类型不同的是，所有的数据都使用了 Tag + 具体数据的编码方式
         * IMPROVE: 应该可以使用 Tag + len + 所有数据的编码方式？可以省下一部分的空间(len - 1) * Tag
         * @param dst
         */
        void VersionEdit::EncodeTo(std::string *dst) const {
            // 类似于以键值对，前面的Varint32表示后面存放的数据的类型，后面存放具体的数据
            // 比如KNextFileNumber就是1，表示后面的64位就是next_file_number
            // 分别存放 next_file_number(可选)
            if (has_next_file_number_) {
                PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
            }
            // 以及column_family_id_
            PutVarint32Varint32(dst, kColumnFamilyID, column_family_id_);

            for (auto &file : added_files_) {
                // 与上述操作类似
                PutVarint32(dst, kAddedBlobFileV2);
                file->EncodeTo(dst);
            }
            for (auto &file : deleted_files_) {
                // obsolete sequence is a inpersistent field, so no need to encode it.
                PutVarint32Varint64(dst, kDeletedBlobFile, file.first);
            }
        }

        /**
         * 从src中解析出VersionEdit
         * 其中delete_files_中的sequenceNumber全部都会变成0？
         * @param src
         * @return
         */
        Status VersionEdit::DecodeFrom(Slice *src) {
            uint32_t tag;
            uint64_t file_number;
            std::shared_ptr<BlobFileMeta> blob_file;
            Status s;

            const char *error = nullptr;
            while (!error && !src->empty()) {
                if (!GetVarint32(src, &tag)) {
                    error = "invalid tag";
                    break;
                }
                switch (tag) {
                    case kNextFileNumber:
                        if (GetVarint64(src, &next_file_number_)) {
                            has_next_file_number_ = true;
                        } else {
                            error = "next file number";
                        }
                        break;
                    case kColumnFamilyID:
                        if (GetVarint32(src, &column_family_id_)) {
                        } else {
                            error = "column family id";
                        }
                        break;
                        // for compatibility issue
                    case kAddedBlobFile:
                        blob_file = std::make_shared<BlobFileMeta>();
                        s = blob_file->DecodeFromLegacy(src);
                        if (s.ok()) {
                            AddBlobFile(blob_file);
                        } else {
                            error = s.ToString().c_str();
                        }
                        break;
                    case kAddedBlobFileV2:
                        blob_file = std::make_shared<BlobFileMeta>();
                        s = blob_file->DecodeFrom(src);
                        if (s.ok()) {
                            AddBlobFile(blob_file);
                        } else {
                            error = s.ToString().c_str();
                        }
                        break;
                    case kDeletedBlobFile:
                        if (GetVarint64(src, &file_number)) {
                            DeleteBlobFile(file_number);
                        } else {
                            error = "deleted blob file";
                        }
                        break;
                    default:
                        error = "unknown tag";
                        break;
                }
            }

            if (error) {
                return Status::Corruption("VersionEdit", error);
            }
            return Status::OK();
        }

        bool operator==(const VersionEdit &lhs, const VersionEdit &rhs) {
            if (lhs.added_files_.size() != rhs.added_files_.size()) {
                return false;
            }
            std::map<uint64_t, std::shared_ptr<BlobFileMeta>> blob_files;
            for (std::size_t idx = 0; idx < lhs.added_files_.size(); idx++) {
                blob_files.insert(
                        {lhs.added_files_[idx]->file_number(), lhs.added_files_[idx]});
            }
            for (std::size_t idx = 0; idx < rhs.added_files_.size(); idx++) {
                auto iter = blob_files.find(rhs.added_files_[idx]->file_number());
                if (iter == blob_files.end() || !(*iter->second == *rhs.added_files_[idx]))
                    return false;
            }

            return (lhs.has_next_file_number_ == rhs.has_next_file_number_ &&
                    lhs.next_file_number_ == rhs.next_file_number_ &&
                    lhs.column_family_id_ == rhs.column_family_id_ &&
                    lhs.deleted_files_ == rhs.deleted_files_);
        }

    } // namespace titandb
} // namespace rocksdb
