//
// Created by shb on 23-8-15.
//

#include "vcf_options.h"

#include "options/options_helper.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/options_type.h"
#include "vector_options.h"
#include "vector_options_helper.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
const std::string OptionsHelper::kVectorCFOptionsName =
    "VectorColumnFamilyOptions";

static std::unordered_map<std::string, OptionTypeInfo>
    vcf_immutable_options_type_info = {
        {"dim",
         {offsetof(struct ImmutableVectorCFOptions, dim), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"space",
         {offsetof(struct ImmutableVectorCFOptions, space), OptionType::kUInt8T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"allow_replace_deleted",
         {offsetof(struct ImmutableVectorCFOptions, allow_replace_deleted),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    vcf_mutable_options_type_info = {
        {"max_elements",
         {offsetof(struct MutableVectorCFOptions, max_elements),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"M",
         {offsetof(struct MutableVectorCFOptions, M), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"ef_construction",
         {offsetof(struct MutableVectorCFOptions, ef_construction),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"random_seed",
         {offsetof(struct MutableVectorCFOptions, random_seed),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"visit_list_pool_size",
         {offsetof(struct MutableVectorCFOptions, visit_list_pool_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"termination_threshold",
         {offsetof(struct MutableVectorCFOptions, termination_threshold),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}}};

void UpdateVectorColumnFamilyOptions(const MutableVectorCFOptions& moptions,
                                     VectorColumnFamilyOptions* cf_opts) {
  cf_opts->max_elements = moptions.max_elements;
  cf_opts->M = moptions.M;
  cf_opts->ef_construction = moptions.ef_construction;
  cf_opts->random_seed = moptions.random_seed;
  cf_opts->visit_list_pool_size = moptions.visit_list_pool_size;
}
void UpdateVectorColumnFamilyOptions(const ImmutableVectorCFOptions& ioptions,
                                     VectorColumnFamilyOptions* cf_opts) {
  cf_opts->dim = ioptions.dim;
  cf_opts->space = ioptions.space;
  cf_opts->allow_replace_deleted = ioptions.allow_replace_deleted;
}

class ConfigurableMutableVectorCFOptions : public Configurable {
 public:
  explicit ConfigurableMutableVectorCFOptions(
      const MutableVectorCFOptions& mvcf) {
    mutable_ = mvcf;
    RegisterOptions(&mutable_, &vcf_mutable_options_type_info);
  }

 protected:
  MutableVectorCFOptions mutable_;
};

class ConfigurableVectorCFOptions : public ConfigurableMutableVectorCFOptions {
 public:
  ConfigurableVectorCFOptions(
      const VectorColumnFamilyOptions& opts,
      const std::unordered_map<std::string, std::string>* map)
      : ConfigurableMutableVectorCFOptions(MutableVectorCFOptions(opts)),
        immutable_(opts),
        vcf_options_(opts),
        opt_map_(map) {
    RegisterOptions(&immutable_, &vcf_immutable_options_type_info);
  }

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override {
    Status s = Configurable::ConfigureOptions(config_options, opts_map, unused);
    if (s.ok()) {
      UpdateVectorColumnFamilyOptions(mutable_, &vcf_options_);
      UpdateVectorColumnFamilyOptions(immutable_, &vcf_options_);
      s = PrepareOptions(config_options);
    }
    return s;
  }

  virtual const void* GetOptionsPtr(const std::string& name) const override {
    if (name == OptionsHelper::kVectorCFOptionsName) {
      return &vcf_options_;
    } else {
      return ConfigurableMutableVectorCFOptions::GetOptionsPtr(name);
    }
  }

  bool OptionsAreEqual(const ConfigOptions& config_options,
                       const OptionTypeInfo& opt_info,
                       const std::string& opt_name, const void* const this_ptr,
                       const void* const that_ptr,
                       std::string* mismatch) const override {
    bool equals = opt_info.AreEqual(config_options, opt_name, this_ptr,
                                    that_ptr, mismatch);
    if (!equals && opt_info.IsByName()) {
      if (opt_map_ == nullptr) {
        equals = true;
      } else {
        const auto& iter = opt_map_->find(opt_name);
        if (iter == opt_map_->end()) {
          equals = true;
        } else {
          equals = opt_info.AreEqualByName(config_options, opt_name, this_ptr,
                                           iter->second);
        }
      }
      if (equals) {  // False alarm, clear mismatch
        *mismatch = "";
      }
    }
    if (equals && opt_info.IsConfigurable() && opt_map_ != nullptr) {
      const auto* this_config = opt_info.AsRawPointer<Configurable>(this_ptr);
      if (this_config == nullptr) {
        const auto& iter = opt_map_->find(opt_name);
        // If the name exists in the map and is not empty/null,
        // then the this_config should be set.
        if (iter != opt_map_->end() && !iter->second.empty() &&
            iter->second != kNullptrString) {
          *mismatch = opt_name;
          equals = false;
        }
      }
    }
    return equals;
  }

 private:
  ImmutableVectorCFOptions immutable_;
  VectorColumnFamilyOptions vcf_options_;
  const std::unordered_map<std::string, std::string>* opt_map_;
};

std::unique_ptr<Configurable> VectorCFOptionsAsConfigurable(
    const MutableVectorCFOptions& opts) {
  std::unique_ptr<Configurable> ptr(
      new ConfigurableMutableVectorCFOptions(opts));
  return ptr;
}

std::unique_ptr<Configurable> VectorCFOptionsAsConfigurable(
    const VectorColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map) {
  std::unique_ptr<Configurable> ptr(
      new ConfigurableVectorCFOptions(opts, opt_map));
  return ptr;
}

Status GetStringFromVectorColumnFamilyOptions(
    std::string* opt_string, const VectorColumnFamilyOptions& vcf_options,
    const std::string& delimiter) {
  ConfigOptions config_options;
  config_options.delimiter = delimiter;
  return GetStringFromVectorColumnFamilyOptions(config_options, vcf_options,
                                                opt_string);
}

Status GetStringFromVectorColumnFamilyOptions(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& vcf_options, std::string* opt_string) {
  const auto config = VectorCFOptionsAsConfigurable(vcf_options);
  return config->GetOptionString(config_options, opt_string);
}

Status GetMutableVectorOptionsFromStrings(
    const MutableVectorCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* /*info_log*/, MutableVectorCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions config_options;
  Status s = OptionTypeInfo::ParseType(
      config_options, options_map, vcf_mutable_options_type_info, new_options);
  if (!s.ok()) {
    *new_options = base_options;
  }
  return s;
}

Status GetStringFromMutableVectorCFOptions(
    const ConfigOptions& config_options,
    const MutableVectorCFOptions& mutable_opts, std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  return OptionTypeInfo::SerializeType(
      config_options, vcf_mutable_options_type_info, &mutable_opts, opt_string);
}
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE