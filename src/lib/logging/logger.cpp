#include "logger.hpp"

#include <boost/range.hpp>
#include <boost/range/algorithm/reverse.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <sstream>

#include "abstract_logger.hpp"
#include "binary_formatter.hpp"
#include "group_commit_logger.hpp"
#include "no_logger.hpp"
#include "simple_logger.hpp"
#include "text_formatter.hpp"
#include "utils/filesystem.hpp"

namespace opossum {

// Logging is initially set to NoLogger and set to an implementation by console or server
Logger::Implementation Logger::_implementation = Implementation::No;

std::string Logger::_data_path = "";                       // NOLINT
const std::string Logger::_log_folder = "logs/";           // NOLINT
std::string Logger::_log_path = _data_path + _log_folder;  // NOLINT
const std::string Logger::_filename = "hyrise-log";        // NOLINT

std::unique_ptr<AbstractLogger> Logger::_logger_instance = std::unique_ptr<NoLogger>(new NoLogger());

AbstractLogger& Logger::get() { return *_logger_instance; }

void Logger::setup(std::string folder, const Implementation implementation, const Format format, const uint64_t flush_interval) {
  DebugAssert(_implementation == Implementation::No, "Logger: Trying to setup logging that has already been setup");
  DebugAssert(folder.length() > 0, "Logger: empty string is no folder");

  Assert((implementation == Implementation::No) == (format == Format::No),
         "Logger: Logger and log format must both either be turned on or off.");

  if (folder[folder.size() - 1] != '/') {
    folder += '/';
  }

  _data_path = folder;
  _log_path = _data_path + _log_folder;

  _create_directories();

  _implementation = implementation;

  std::unique_ptr<AbstractFormatter> formatter;
  switch (format) {
    case Format::No: {
      break;
    }
    case Format::Text: {
      formatter = std::unique_ptr<TextFormatter>(new TextFormatter());
      break;
    }
    case Format::Binary: {
      formatter = std::unique_ptr<BinaryFormatter>(new BinaryFormatter());
      break;
    }
    default: { throw std::runtime_error("Logger: format unkown."); }
  }

  switch (_implementation) {
    case Implementation::No: {
      // _logger_instance is initiated with NoLogger
      break;
    }
    case Implementation::Simple: {
      _logger_instance = std::unique_ptr<SimpleLogger>(new SimpleLogger(std::move(formatter)));
      break;
    }
    case Implementation::GroupCommit: {
      _logger_instance = std::unique_ptr<GroupCommitLogger>(new GroupCommitLogger(std::move(formatter), flush_interval));
      break;
    }
    default: { throw std::runtime_error("Logger: implementation unkown."); }
  }
}

void Logger::reset_to_no_logger() {
  _implementation = Implementation::No;
  _logger_instance = std::unique_ptr<NoLogger>(new NoLogger());
}

bool Logger::is_active() { return _implementation != Implementation::No; }

void Logger::delete_log_files() {
  filesystem::remove_all(_log_path);
  _create_directories();
}

void Logger::_create_directories() {
  filesystem::create_directory(_data_path);
  filesystem::create_directory(_log_path);
}

std::string Logger::get_new_log_path() {
  auto log_number = _get_latest_log_number() + 1;
  std::string path = _log_path + _filename + std::to_string(log_number);
  return path;
}

std::vector<std::string> Logger::get_all_log_file_paths() {
  DebugAssert(filesystem::exists(_log_path), "Logger: Log path does not exist.");
  std::vector<std::string> result;
  for (auto& path : boost::make_iterator_range(filesystem::directory_iterator(_log_path), {})) {
    auto pos = path.path().string().rfind(_filename);
    if (pos == std::string::npos) {
      continue;
    }
    result.push_back(path.path().string());
  }

  if (result.size() > 0) {
    auto pos = result[0].rfind(_filename) + _filename.length();
    std::sort(result.begin(), result.end(),
              [&pos](std::string a, std::string b) { return std::stoul(a.substr(pos)) < std::stoul(b.substr(pos)); });
  }
  return (result);
}

u_int32_t Logger::_get_latest_log_number() {
  u_int32_t max_number{0};

  for (auto& path : boost::make_iterator_range(filesystem::directory_iterator(_log_path), {})) {
    auto pos = path.path().string().rfind(_filename);
    if (pos == std::string::npos) {
      continue;
    }

    u_int32_t number = std::stoul(path.path().string().substr(pos + _filename.length()));
    max_number = std::max(max_number, number);
  }
  return max_number;
}

}  // namespace opossum
