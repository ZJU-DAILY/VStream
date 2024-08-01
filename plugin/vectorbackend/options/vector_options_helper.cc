//
// Created by shb on 23-8-15.
//

#include "vector_options_helper.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
Status ValidateOptions(const DBOptions& db_opts,
                       const VectorColumnFamilyOptions& vcf_opts) {
  Status s;
  auto db_cfg = DBOptionsAsConfigurable(db_opts);
  auto vcf_cfg = VectorCFOptionsAsConfigurable(vcf_opts);
  // TODO: Is it necessary to rewrite ValidateOptions() to support
  // VectorColumnFamilyOptions?
  s = db_cfg->ValidateOptions(
      db_opts, reinterpret_cast<const ColumnFamilyOptions&>(vcf_opts));
  if (s.ok())
    s = vcf_cfg->ValidateOptions(
        db_opts, reinterpret_cast<const ColumnFamilyOptions&>(vcf_opts));
  return s;
}

template <typename T>
Status ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opt_map,
    const std::string& option_name, Configurable* config, T* new_opts) {
  Status s = config->ConfigureFromMap(config_options, opt_map);
  if (s.ok()) {
    *new_opts = *(config->GetOptions<T>(option_name));
  }
  return s;
}

Status GetVectorColumnFamilyOptionsFromMap(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    VectorColumnFamilyOptions* new_options) {
  assert(new_options);

  *new_options = base_options;

  const auto config = VectorCFOptionsAsConfigurable(base_options);
  Status s = ConfigureFromMap<VectorColumnFamilyOptions>(
      config_options, opts_map, OptionsHelper::kVectorCFOptionsName,
      config.get(), new_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

Status GetVectorColumnFamilyOptionsFromString(
    const ConfigOptions& config_options,
    const VectorColumnFamilyOptions& base_options, const std::string& opts_str,
    VectorColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetVectorColumnFamilyOptionsFromMap(config_options, base_options,
                                             opts_map, new_options);
}
}  //  namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE