#include <json.hpp>

#include <iostream>

#include "utils/singleton.hpp"

namespace opossum {

// singleton
class JitEvaluationHelper : public Singleton<JitEvaluationHelper> {
 public:
  nlohmann::json& experiment() { return _experiment; }
  nlohmann::json& globals() { return _globals; }
  nlohmann::json& queries() { return _queries; }
  nlohmann::json& result() { return _result; }

 private:
  nlohmann::json _experiment;
  nlohmann::json _globals;
  nlohmann::json _queries;
  nlohmann::json _result;

  Global() = default;

  friend class Singleton;
};

}  // namespace opossum
