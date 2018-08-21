
namespace opossum {

struct Global {
  static Global& get();

  bool jit = false;
  bool lazy_load = false;
  bool jit_validate = false;
  bool deep_copy_exists = false;
};

}  // namespace opossum
