#ifndef Utils_h
#define Utils_h

#include <string>
#include <random>

namespace mozilla {
namespace Utils {

bool EnsurePath(const std::string& aPath);

/** Simple class for generation UUIDs version 4 */
class UUIDGenerator
{
public:
  UUIDGenerator();

  /** Get a UUID */
  const std::string& GetUUID();

private:
  std::mt19937 mGenerator;
  std::uniform_int_distribution<char> mDistribution;
  std::string mUUID;
};

} // namespace Utils
} // namespace mozilla

#endif // Utils_h
